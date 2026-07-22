from __future__ import annotations

import asyncio
import logging

from ..config import HarborCameraConfig
from ..data.mqtt_models import SettingsEvent
from ..device import HarborDevice
from ..events import (
    CameraEventUpdate,
    HarborEvent,
    LocalLivekitHeartbeatUpdate,
    SettingsUpdate,
    ViewerInfo,
    ViewerJoinedUpdate,
    ViewerLeftUpdate,
)
from ..state import HarborEventState, HarborViewer

_LOGGER = logging.getLogger(__name__)

DEFAULT_CAMERA_EVENT_KEYS = ("motion_detection", "cry_detection", "noise_detection")
DEFAULT_EVENT_ACTIVE_SECONDS = 5.0

# Known values for enumerated state fields. The device reports these in
# mixed/upper case (e.g. "PLAYING", "GOOD"); they are normalized to the
# lowercase values below before being stored in ``HarborDeviceState.values``.
# A value outside these sets is mapped to ``UNKNOWN_ENUM_VALUE`` (and logged
# once), so a stored enum value is always a member of its set. Consumers can
# rely on that guarantee instead of clamping the value themselves.
UNKNOWN_ENUM_VALUE = "unknown"
SPEAKER_STATES = frozenset({"idle", "muted", "off", "paused", "playing", "unknown"})
STREAM_QUALITIES = frozenset({"excellent", "fair", "good", "poor", "unknown"})


class HarborCamera(HarborDevice):
    """Represents a Harbor camera device."""

    def __init__(self, config: HarborCameraConfig) -> None:
        """Initialize the camera device."""
        super().__init__(config.serial, "camera")
        self.config = config
        self._event_reset_handles: dict[str, asyncio.TimerHandle] = {}
        self._unexpected_enum_values: set[tuple[str, str]] = set()

        for event_key in DEFAULT_CAMERA_EVENT_KEYS:
            self._ensure_camera_event(event_key)

    def get_topics(self) -> list[str]:
        """Return topics that should be subscribed for this device."""

        return [
            f"cameras/{self.serial}/events/#",
            f"cameras/{self.serial}/responses/#",
        ]

    def _apply_event(self, event: HarborEvent) -> None:
        """Apply a Harbor event to camera state, including camera-only events."""
        super()._apply_event(event)
        match event:
            case LocalLivekitHeartbeatUpdate(payload=payload, viewers=viewers):
                self._apply_local_livekit_heartbeat(payload, viewers)
            case SettingsUpdate(payload=payload):
                self._apply_camera_settings(payload)
            case ViewerJoinedUpdate(viewer=viewer):
                self._apply_viewer_joined(viewer)
            case ViewerLeftUpdate(viewer_id=viewer_id):
                self._apply_viewer_left(viewer_id)
            case CameraEventUpdate():
                self._apply_camera_event(event)

    def _apply_camera_settings(self, payload: SettingsEvent) -> None:
        """Apply camera controls exposed by a settings payload."""
        if payload.settings is not None and payload.settings.preference_stream_paused is not None:
            self.state.values["camera_on"] = not payload.settings.preference_stream_paused
        if payload.state is not None and payload.state.video_night_mode is not None:
            self.state.values["night_mode"] = payload.state.video_night_mode

    def _apply_local_livekit_heartbeat(
        self,
        payload,
        viewers: tuple[ViewerInfo, ...],
    ) -> None:
        """Apply a local LiveKit heartbeat payload."""
        self._set_state_value("bitrate", payload.bitrate)
        self._set_state_value("wifi_strength", payload.network_bars)
        self._set_state_value("camera_present", payload.camera_present)
        self._set_state_value(
            "speaker_state",
            self._normalize_enum_value("speaker_state", payload.speaker_state, SPEAKER_STATES),
        )
        self._set_state_value(
            "stream_quality",
            self._normalize_enum_value("stream_quality", payload.stream_quality, STREAM_QUALITIES),
        )
        self._set_state_value("app_start_time", payload.app_start_time)
        self._set_state_value("stream_start_time", payload.stream_start_time)

        self.state.viewers = {
            viewer.viewer_id: HarborViewer(
                viewer_id=viewer.viewer_id,
                identity=viewer.identity,
                client=viewer.client,
                is_local=viewer.is_local,
                role=viewer.role,
            )
            for viewer in viewers
        }
        self.state.values["num_viewers"] = len(self.state.viewers)

    def _normalize_enum_value(
        self,
        field_name: str,
        value: str | None,
        known_values: frozenset[str],
    ) -> str | None:
        """Normalize an enumerated device value to a known lowercase member.

        Returns ``None`` only when the device omitted the field. A value that
        does not match ``known_values`` is mapped to ``UNKNOWN_ENUM_VALUE`` so
        callers can rely on the result always being a member of the set.
        """
        if value is None:
            return None
        normalized = value.strip().lower()
        if not normalized:
            return None
        if normalized not in known_values:
            if (field_name, normalized) not in self._unexpected_enum_values:
                self._unexpected_enum_values.add((field_name, normalized))
                _LOGGER.warning(
                    "Camera %s reported unexpected %s value %r (known values: %s)",
                    self.serial,
                    field_name,
                    normalized,
                    sorted(known_values),
                )
            return UNKNOWN_ENUM_VALUE
        return normalized

    def _apply_viewer_joined(self, viewer: ViewerInfo | None) -> None:
        """Apply a viewer joined update."""
        if viewer is None:
            return

        self.state.viewers[viewer.viewer_id] = HarborViewer(
            viewer_id=viewer.viewer_id,
            identity=viewer.identity,
            client=viewer.client,
            is_local=viewer.is_local,
            role=viewer.role,
        )
        self.state.values["num_viewers"] = len(self.state.viewers)

    def _apply_viewer_left(self, viewer_id: str | None) -> None:
        """Apply a viewer left update."""
        if viewer_id is None:
            return

        self.state.viewers.pop(viewer_id, None)
        self.state.values["num_viewers"] = len(self.state.viewers)

    def _apply_camera_event(self, event: CameraEventUpdate) -> None:
        """Apply a transient camera event update."""
        event_state = self._ensure_camera_event(event.event_key, topic=event.topic)
        event_state.topic = event.topic
        event_state.last_seen = event.timestamp
        event_state.last_payload = event.raw_payload

        if handle := self._event_reset_handles.pop(event.event_key, None):
            handle.cancel()

        if event.explicit_state is False:
            event_state.is_on = False
            return

        event_state.is_on = True
        active_seconds = event.active_seconds
        if active_seconds <= 0:
            active_seconds = DEFAULT_EVENT_ACTIVE_SECONDS

        loop = asyncio.get_running_loop()
        self._event_reset_handles[event.event_key] = loop.call_later(
            active_seconds,
            lambda: asyncio.create_task(self._async_reset_event(event.event_key)),
        )
        _LOGGER.debug(
            "Camera %s received event on topic %s: %s",
            self.serial,
            event.topic,
            event.raw_payload,
        )

    def shutdown(self) -> None:
        """Release camera resources."""
        for handle in self._event_reset_handles.values():
            handle.cancel()
        self._event_reset_handles.clear()

    async def _async_reset_event(self, event_key: str) -> None:
        """Reset a transient event to off after its active period."""
        self._event_reset_handles.pop(event_key, None)
        if event_state := self.state.events.get(event_key):
            event_state.is_on = False
            await self._emit_update()

    def _ensure_camera_event(
        self,
        event_key: str,
        *,
        topic: str | None = None,
    ) -> HarborEventState:
        """Ensure an event state exists for the camera."""
        if existing := self.state.events.get(event_key):
            if topic is not None:
                existing.topic = topic
            return existing

        event_state = HarborEventState(
            key=event_key,
            topic=topic or event_key.replace("_", "-"),
            friendly_name=_event_name_from_key(event_key),
        )
        self.state.events[event_key] = event_state
        return event_state


def _event_name_from_key(event_key: str) -> str:
    """Return a user-facing event name for an event key."""
    if event_key == "cry_detection":
        return "Cry detected"
    if event_key == "motion_detection":
        return "Motion detected"
    if event_key == "noise_detection":
        return "Noise detected"

    words = event_key.replace("_", " ").strip()
    return words[:1].upper() + words[1:] if words else event_key
