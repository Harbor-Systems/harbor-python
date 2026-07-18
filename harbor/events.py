from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal, TypeVar, cast, overload

from pydantic import BaseModel, ValidationError

from .data.mqtt_models import (
    HeartbeatEvent,
    LocalLivekitHeartbeatEvent,
    MotionDetectedEvent,
    SettingsEvent,
    ViewerJoinedEvent,
    ViewerLeftEvent,
)

_LOGGER = logging.getLogger(__name__)

HarborSourceType = Literal["camera", "monitor"]


class EventType(StrEnum):
    """Normalized Harbor event types."""

    HEARTBEAT = "heartbeat"
    LOCAL_LIVEKIT_HEARTBEAT = "local_livekit_heartbeat"
    VIEWER_JOINED = "viewer_joined"
    VIEWER_LEFT = "viewer_left"
    SETTINGS = "settings"
    MOTION_DETECTION = "motion_detection"
    CAMERA_EVENT = "camera_event"
    RAW = "raw"


@dataclass(slots=True, frozen=True, kw_only=True)
class ViewerInfo:
    """Normalized viewer data derived from Harbor payloads."""

    viewer_id: str
    identity: str | None = None
    client: str | None = None
    is_local: bool | None = None
    role: str | None = None


@dataclass(slots=True, frozen=True, kw_only=True)
class HarborEvent:
    """Base Harbor event emitted to subscribers."""

    source_sn: str
    source_type: HarborSourceType
    topic: str
    event_key: str
    timestamp: datetime
    raw_payload: Any
    app_version: str | None = None
    os_version: str | None = None
    display_name: str | None = None
    event_type: EventType = field(init=False, default=EventType.RAW)


@dataclass(slots=True, frozen=True, kw_only=True)
class RawEventUpdate(HarborEvent):
    """A topic that could be parsed but did not match a typed payload."""

    payload: Any


@dataclass(slots=True, frozen=True, kw_only=True)
class HeartbeatUpdate(HarborEvent):
    """A typed heartbeat update."""

    payload: HeartbeatEvent
    event_type: EventType = field(init=False, default=EventType.HEARTBEAT)


@dataclass(slots=True, frozen=True, kw_only=True)
class LocalLivekitHeartbeatUpdate(HarborEvent):
    """A typed local LiveKit heartbeat update."""

    payload: LocalLivekitHeartbeatEvent
    viewers: tuple[ViewerInfo, ...]
    event_type: EventType = field(init=False, default=EventType.LOCAL_LIVEKIT_HEARTBEAT)


@dataclass(slots=True, frozen=True, kw_only=True)
class ViewerJoinedUpdate(HarborEvent):
    """A typed viewer joined update."""

    payload: ViewerJoinedEvent
    viewer: ViewerInfo | None
    event_type: EventType = field(init=False, default=EventType.VIEWER_JOINED)


@dataclass(slots=True, frozen=True, kw_only=True)
class ViewerLeftUpdate(HarborEvent):
    """A typed viewer left update."""

    payload: ViewerLeftEvent
    viewer_id: str | None
    event_type: EventType = field(init=False, default=EventType.VIEWER_LEFT)


@dataclass(slots=True, frozen=True, kw_only=True)
class SettingsUpdate(HarborEvent):
    """A typed settings update."""

    payload: SettingsEvent
    event_type: EventType = field(init=False, default=EventType.SETTINGS)


@dataclass(slots=True, frozen=True, kw_only=True)
class CameraEventUpdate(HarborEvent):
    """A camera event that should be treated like a transient trigger."""

    payload: Any
    active_seconds: float
    explicit_state: bool | None
    event_type: EventType = field(init=False, default=EventType.CAMERA_EVENT)


@dataclass(slots=True, frozen=True, kw_only=True)
class MotionDetectedUpdate(CameraEventUpdate):
    """A typed motion detection update."""

    payload: MotionDetectedEvent
    event_type: EventType = field(init=False, default=EventType.MOTION_DETECTION)


EVENT_MESSAGE_MAP: dict[type[BaseModel], type[HarborEvent]] = {
    HeartbeatEvent: HeartbeatUpdate,
    LocalLivekitHeartbeatEvent: LocalLivekitHeartbeatUpdate,
    ViewerJoinedEvent: ViewerJoinedUpdate,
    ViewerLeftEvent: ViewerLeftUpdate,
    SettingsEvent: SettingsUpdate,
    MotionDetectedEvent: MotionDetectedUpdate,
}


CallbackType = Callable[[HarborEvent], Any]
EventT = TypeVar("EventT", bound=HarborEvent)


def event_key_from_topic(event_name: str) -> str:
    """Normalize an event topic segment into a stable key."""

    event_key = event_name.strip().replace("-", "_")
    if event_key in {"motion_detected", "cry_detected", "noise_detected"}:
        return event_key.removesuffix("_detected") + "_detection"
    return event_key


def parse_payload(payload: Any) -> Any:
    """Decode JSON payloads when they arrive as strings."""

    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return payload
    return payload


def parse_topic(
    topic: str,
) -> tuple[HarborSourceType | None, str | None, str | None]:
    """Parse a Harbor MQTT topic into source type, serial, and event key."""

    parts = topic.split("/")
    if len(parts) < 4:
        return None, None, None

    root, serial, event_root, *rest = parts
    if event_root not in {"events", "responses"} or not rest:
        return None, None, None

    if root == "cameras":
        return "camera", serial, event_key_from_topic(rest[-1])

    if root == "monitors":
        return "monitor", serial, event_key_from_topic(rest[-1])

    return None, None, None


def extract_event_duration_seconds(payload: Any) -> float:
    """Extract an event duration from a payload."""

    default = 5.0
    if not isinstance(payload, Mapping):
        return default

    duration = payload.get("duration")
    if duration is None or isinstance(duration, bool):
        return default

    if isinstance(duration, int | float):
        return float(duration) if duration > 0 else default

    if not isinstance(duration, str):
        return default

    duration = duration.strip()
    if not duration:
        return default

    try:
        parsed = float(duration)
    except ValueError:
        parts = duration.split(":")
        if len(parts) not in (2, 3):
            return default
        try:
            numbers = [float(part) for part in parts]
        except ValueError:
            return default
        if len(numbers) == 2:
            minutes, seconds = numbers
            total = minutes * 60 + seconds
        else:
            hours, minutes, seconds = numbers
            total = hours * 3600 + minutes * 60 + seconds
        return total if total > 0 else default

    return parsed if parsed > 0 else default


def extract_explicit_event_state(payload: Any) -> bool | None:
    """Extract an explicit on/off state from a payload when available."""

    if not isinstance(payload, Mapping):
        return None

    for key in (
        "active",
        "detected",
        "is_active",
        "is_detected",
        "motion",
        "motion_detected",
        "cry",
        "cry_detected",
        "noise",
        "noise_detected",
    ):
        value = payload.get(key)
        if isinstance(value, bool):
            return value

    state_value = payload.get("state")
    if isinstance(state_value, str):
        normalized = state_value.strip().lower()
        if normalized in {"active", "detected", "on", "true"}:
            return True
        if normalized in {"inactive", "off", "false", "idle"}:
            return False

    return None


def parse_message(
    topic: str,
    payload: Any,
    *,
    timestamp: datetime | None = None,
) -> HarborEvent | None:
    """Parse a raw Harbor MQTT message into a typed Harbor event."""

    source_type, serial, event_key = parse_topic(topic)
    if source_type is None or serial is None or event_key is None:
        return None

    raw_payload = parse_payload(payload)
    event_timestamp = timestamp or datetime.now(UTC)
    app_version, os_version, display_name = _extract_metadata(raw_payload)
    base_kwargs = {
        "source_sn": serial,
        "source_type": source_type,
        "topic": topic,
        "event_key": event_key,
        "timestamp": event_timestamp,
        "raw_payload": raw_payload,
        "app_version": app_version,
        "os_version": os_version,
        "display_name": display_name,
    }

    if event_key == "heartbeat":
        if heartbeat_payload := _validate_payload(HeartbeatEvent, raw_payload, topic):
            return HeartbeatUpdate(payload=heartbeat_payload, **base_kwargs)
        return RawEventUpdate(payload=raw_payload, **base_kwargs)

    if event_key == "local_livekit_heartbeat":
        if livekit_payload := _validate_payload(LocalLivekitHeartbeatEvent, raw_payload, topic):
            return LocalLivekitHeartbeatUpdate(
                payload=livekit_payload,
                viewers=_extract_viewers_from_local_livekit(livekit_payload),
                **base_kwargs,
            )
        return RawEventUpdate(payload=raw_payload, **base_kwargs)

    if event_key == "viewer_joined":
        if viewer_joined_payload := _validate_payload(ViewerJoinedEvent, raw_payload, topic):
            return ViewerJoinedUpdate(
                payload=viewer_joined_payload,
                viewer=_extract_viewer_info_from_payload(raw_payload)
                or _extract_viewer_info(
                    viewer_joined_payload.viewer_id,
                    viewer_joined_payload.identity,
                    viewer_joined_payload.client,
                    viewer_joined_payload.is_local,
                    viewer_joined_payload.role,
                ),
                **base_kwargs,
            )
        return RawEventUpdate(payload=raw_payload, **base_kwargs)

    if event_key == "viewer_left":
        if viewer_left_payload := _validate_payload(ViewerLeftEvent, raw_payload, topic):
            return ViewerLeftUpdate(
                payload=viewer_left_payload,
                viewer_id=_extract_viewer_id_from_payload(raw_payload)
                or _coalesce_string(
                    viewer_left_payload.viewer_id,
                    viewer_left_payload.identity,
                    viewer_left_payload.client,
                ),
                **base_kwargs,
            )
        return RawEventUpdate(payload=raw_payload, **base_kwargs)

    if event_key in {"settings", "get_settings"}:
        if settings_payload := _validate_payload(SettingsEvent, raw_payload, topic):
            return SettingsUpdate(payload=settings_payload, **base_kwargs)
        return RawEventUpdate(payload=raw_payload, **base_kwargs)

    if source_type == "camera":
        active_seconds = extract_event_duration_seconds(raw_payload)
        explicit_state = extract_explicit_event_state(raw_payload)

        if event_key == "motion_detection":
            if motion_payload := _validate_payload(MotionDetectedEvent, raw_payload, topic):
                return MotionDetectedUpdate(
                    payload=motion_payload,
                    active_seconds=active_seconds,
                    explicit_state=explicit_state,
                    **base_kwargs,
                )

        return CameraEventUpdate(
            payload=raw_payload,
            active_seconds=active_seconds,
            explicit_state=explicit_state,
            **base_kwargs,
        )

    return RawEventUpdate(payload=raw_payload, **base_kwargs)


class SubscriptionManager:
    """Manage event subscriptions for typed Harbor events."""

    def __init__(self) -> None:
        """Initialize the subscription manager."""

        self._subscribers: dict[type[HarborEvent], list[CallbackType]] = defaultdict(list)
        self._all_subscribers: list[CallbackType] = []

    @overload
    def subscribe(
        self,
        event_type: type[EventT],
        callback: Callable[[EventT], Any],
    ) -> Callable[[], None]: ...

    @overload
    def subscribe(
        self,
        event_type: type[BaseModel],
        callback: CallbackType,
    ) -> Callable[[], None]: ...

    @overload
    def subscribe(
        self,
        event_type: None,
        callback: CallbackType,
    ) -> Callable[[], None]: ...

    def subscribe(
        self,
        event_type: type[HarborEvent] | type[BaseModel] | None,
        callback: Callable[[Any], Any],
    ) -> Callable[[], None]:
        """Subscribe to a Harbor event type or to all events."""

        typed_callback = cast(CallbackType, callback)
        if event_type is None:
            self._all_subscribers.append(typed_callback)

            def unsubscribe_all() -> None:
                if typed_callback in self._all_subscribers:
                    self._all_subscribers.remove(typed_callback)

            return unsubscribe_all

        resolved_type = _resolve_event_type(event_type)
        self._subscribers[resolved_type].append(typed_callback)

        def unsubscribe_event() -> None:
            if typed_callback in self._subscribers[resolved_type]:
                self._subscribers[resolved_type].remove(typed_callback)

        return unsubscribe_event

    async def emit(self, event: HarborEvent) -> None:
        """Emit an event to all matching subscribers."""

        seen_callbacks: set[int] = set()
        callbacks: list[CallbackType] = []

        for base_type in type(event).__mro__:
            if not isinstance(base_type, type) or not issubclass(base_type, HarborEvent):
                continue
            for callback in self._subscribers.get(cast(type[HarborEvent], base_type), []):
                callback_id = id(callback)
                if callback_id in seen_callbacks:
                    continue
                seen_callbacks.add(callback_id)
                callbacks.append(callback)

        for callback in self._all_subscribers:
            callback_id = id(callback)
            if callback_id in seen_callbacks:
                continue
            seen_callbacks.add(callback_id)
            callbacks.append(callback)

        for callback in callbacks:
            try:
                result = callback(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                _LOGGER.exception(
                    "Error in event callback %s for %s",
                    getattr(callback, "__qualname__", repr(callback)),
                    event.event_key,
                )


class HarborEventBus:
    """Parse raw MQTT messages and emit typed Harbor events."""

    def __init__(self) -> None:
        """Initialize the Harbor event bus."""

        self._subscriptions = SubscriptionManager()

    @overload
    def subscribe(
        self,
        event_type: type[EventT],
        callback: Callable[[EventT], Any],
    ) -> Callable[[], None]: ...

    @overload
    def subscribe(
        self,
        event_type: type[BaseModel],
        callback: CallbackType,
    ) -> Callable[[], None]: ...

    @overload
    def subscribe(
        self,
        event_type: None,
        callback: CallbackType,
    ) -> Callable[[], None]: ...

    def subscribe(
        self,
        event_type: type[HarborEvent] | type[BaseModel] | None,
        callback: Callable[[Any], Any],
    ) -> Callable[[], None]:
        """Subscribe to typed Harbor events."""

        return self._subscriptions.subscribe(event_type, callback)

    async def async_process_message(self, topic: str, payload: Any) -> HarborEvent | None:
        """Parse a raw MQTT message and emit a typed event when possible."""

        if event := parse_message(topic, payload):
            await self._subscriptions.emit(event)
            return event
        return None


def _resolve_event_type(event_type: type[HarborEvent] | type[BaseModel]) -> type[HarborEvent]:
    """Resolve a subscription type into a Harbor event class."""

    if issubclass(event_type, HarborEvent):
        return event_type

    if event_type in EVENT_MESSAGE_MAP:
        return EVENT_MESSAGE_MAP[event_type]

    raise ValueError(f"Unknown event type: {event_type}")


PayloadT = TypeVar("PayloadT", bound=BaseModel)


def _validate_payload(payload_type: type[PayloadT], payload: Any, topic: str) -> PayloadT | None:
    """Validate a payload with a Harbor payload model."""

    try:
        return payload_type.model_validate(payload)
    except ValidationError:
        _LOGGER.debug("Unable to validate payload for topic %s", topic, exc_info=True)
        return None


def _extract_metadata(payload: Any) -> tuple[str | None, str | None, str | None]:
    """Extract metadata shared across Harbor messages."""

    if not isinstance(payload, Mapping):
        return None, None, None

    app_version = _coerce_string(payload.get("app_version"))
    os_version = _coerce_string(payload.get("os_version"))

    display_name = None
    settings = payload.get("settings")
    if isinstance(settings, Mapping):
        display_name = _coerce_string(settings.get("preference_display_name"))

    return app_version, os_version, display_name


def _extract_viewers_from_local_livekit(
    payload: LocalLivekitHeartbeatEvent,
) -> tuple[ViewerInfo, ...]:
    """Normalize viewer data from a local LiveKit heartbeat."""

    viewers: list[ViewerInfo] = []

    for viewer_key, viewer_payload in payload.viewers_by_identity_full.items():
        if not isinstance(viewer_payload, Mapping):
            continue
        viewer_id = _coalesce_string(
            viewer_payload.get("identity"),
            viewer_payload.get("viewer_id"),
            viewer_payload.get("client"),
            str(viewer_key),
        )
        if viewer_id is None:
            continue
        viewers.append(
            ViewerInfo(
                viewer_id=viewer_id,
                identity=_coalesce_string(viewer_payload.get("identity"), viewer_id),
                client=_coerce_string(viewer_payload.get("client")),
                is_local=_coerce_bool(viewer_payload.get("is_local")),
                role=_coerce_string(viewer_payload.get("role")),
            )
        )

    if viewers:
        return tuple(viewers)

    for viewer_key in payload.viewers_by_identity:
        viewer_id = str(viewer_key)
        viewers.append(ViewerInfo(viewer_id=viewer_id, identity=viewer_id))

    return tuple(viewers)


def _extract_viewer_info(
    viewer_id: Any,
    identity: Any,
    client: Any,
    is_local: Any,
    role: Any,
) -> ViewerInfo | None:
    """Normalize a single viewer event payload."""

    normalized_viewer_id = _coalesce_string(identity, viewer_id, client)
    if normalized_viewer_id is None:
        return None

    return ViewerInfo(
        viewer_id=normalized_viewer_id,
        identity=_coalesce_string(identity, normalized_viewer_id),
        client=_coerce_string(client),
        is_local=_coerce_bool(is_local),
        role=_coerce_string(role),
    )


def _extract_viewer_info_from_payload(payload: Any) -> ViewerInfo | None:
    """Normalize viewer data from top-level or nested viewer payloads."""

    if not isinstance(payload, Mapping):
        return None

    viewer_payload: Mapping[str, Any] = payload
    for key in ("viewer", "participant", "viewer_info", "data"):
        nested = payload.get(key)
        if isinstance(nested, Mapping):
            viewer_payload = nested
            break

    return _extract_viewer_info(
        _coalesce_string(viewer_payload.get("viewer_id"), viewer_payload.get("id")),
        viewer_payload.get("identity"),
        viewer_payload.get("client"),
        viewer_payload.get("is_local"),
        viewer_payload.get("role"),
    )


def _extract_viewer_id_from_payload(payload: Any) -> str | None:
    """Extract a viewer id from top-level or nested viewer payloads."""

    if viewer := _extract_viewer_info_from_payload(payload):
        return viewer.viewer_id
    return None


def _coerce_bool(value: Any) -> bool | None:
    """Convert a value to bool when possible."""

    if isinstance(value, bool):
        return value
    return None


def _coerce_string(value: Any) -> str | None:
    """Convert a value to a non-empty string when possible."""

    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _coalesce_string(*values: Any) -> str | None:
    """Return the first non-empty string-like value."""

    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                return stripped
            continue
        return str(value)
    return None
