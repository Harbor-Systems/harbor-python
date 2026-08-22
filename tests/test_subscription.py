from __future__ import annotations

import asyncio

from harbor.config import HarborCameraConfig
from harbor.devices.camera import HarborCamera
from harbor.events import (
    CameraEventUpdate,
    EventType,
    HarborEvent,
    HarborEventBus,
    HeartbeatUpdate,
    MotionDetectedUpdate,
    NoiseDetectedUpdate,
    SettingsUpdate,
    ViewerJoinedUpdate,
)

REAL_NOISE_PAYLOAD = {
    "active_config": "primary",
    "baseline": "-47.838640dB",
    "baseline_reference": "-47.838640dB",
    "duration": "10s",
    "file_duration": "10.000000s",
    "filename": "sound-anomaly-2026-08-18_19-10-24-516024038.mp4",
    "level": "-36.401137dB",
    "sensitivity": "0",
    "threshold": "-30.000000dB",
    "thumbnail": "sound-anomaly-2026-08-18_19-10-24-516024038.jpeg",
    "timestamp": "2026-08-18T19:10:24.516024038Z",
    "user_offset": "-30.000000dB",
}


def _create_camera() -> HarborCamera:
    """Create a test camera."""

    return HarborCamera(
        HarborCameraConfig(
            serial="TEST123",
            cert_path="/path/to/cert.pem",
            key_path="/path/to/key.pem",
            cert_dir="/path/to/cert_dir",
        )
    )


async def test_subscribe_to_specific_event() -> None:
    """Typed subscribers should receive the concrete update type."""

    camera = _create_camera()
    events_received: list[HeartbeatUpdate] = []

    async def on_heartbeat(event: HarborEvent) -> None:
        assert isinstance(event, HeartbeatUpdate)
        events_received.append(event)

    unsubscribe = camera.subscribe(HeartbeatUpdate, on_heartbeat)

    await camera.handle_message(
        "cameras/TEST123/events/heartbeat",
        {
            "app_version": "1.0.0",
            "os_version": "1.0",
            "temperature": 25.0,
        },
    )

    assert len(events_received) == 1
    assert events_received[0].event_type is EventType.HEARTBEAT
    assert events_received[0].payload.temperature == 25.0
    assert events_received[0].app_version == "1.0.0"

    unsubscribe()

    await camera.handle_message(
        "cameras/TEST123/events/heartbeat",
        {
            "app_version": "1.0.0",
            "os_version": "1.0",
            "temperature": 25.0,
        },
    )

    assert len(events_received) == 1


async def test_subscribe_to_all_events() -> None:
    """All-event subscribers should receive each concrete event type."""

    camera = _create_camera()
    events_received: list[HarborEvent] = []

    async def on_any_event(event: HarborEvent) -> None:
        events_received.append(event)

    unsubscribe = camera.subscribe(None, on_any_event)

    await camera.handle_message(
        "cameras/TEST123/events/heartbeat",
        {
            "app_version": "1.0.0",
            "os_version": "1.0",
            "temperature": 25.0,
        },
    )
    await camera.handle_message(
        "cameras/TEST123/events/viewer-joined",
        {
            "client": "client1",
            "identity": "viewer1",
            "is_local": True,
            "role": "viewer",
            "viewer_id": "viewer1",
        },
    )

    assert len(events_received) == 2
    assert isinstance(events_received[0], HeartbeatUpdate)
    assert isinstance(events_received[1], ViewerJoinedUpdate)

    unsubscribe()


async def test_get_settings_response_updates_camera_display_name() -> None:
    """responses/get-settings should parse as settings and update friendly name."""

    camera = _create_camera()
    events_received: list[SettingsUpdate] = []
    camera.subscribe(SettingsUpdate, lambda event: events_received.append(event))

    await camera.handle_message(
        "cameras/TEST123/responses/get-settings",
        {
            "seq": "seq-1",
            "client": "test-client",
            "triggeredBy": "users/user1",
            "isUpdating": False,
            "settings": {"preference_display_name": "Nursery"},
            "state": {"network_bars": 3, "temperature": 22.5},
        },
    )

    assert len(events_received) == 1
    assert events_received[0].event_type is EventType.SETTINGS
    assert events_received[0].event_key == "get_settings"
    assert camera.state.display_name == "Nursery"
    assert camera.state.values["wifi_strength"] == 3
    assert camera.state.values["temperature"] == 22.5


def test_event_bus_parses_unknown_camera_events() -> None:
    """Camera topics without a typed payload should still normalize."""

    events_received: list[CameraEventUpdate] = []
    event_bus = HarborEventBus()

    def on_camera_event(event: HarborEvent) -> None:
        assert isinstance(event, CameraEventUpdate)
        events_received.append(event)

    event_bus.subscribe(CameraEventUpdate, on_camera_event)

    async def _run() -> None:
        await event_bus.async_process_message(
            "cameras/TEST123/events/operating-mode-changed",
            {"mode": "care"},
        )

    import asyncio

    asyncio.run(_run())

    assert len(events_received) == 1
    assert events_received[0].event_key == "operating_mode_changed"


async def test_motion_detection_keeps_typed_payload() -> None:
    """A motion-detected topic should keep its typed payload and reach base subscribers."""

    camera = _create_camera()
    typed_events: list[MotionDetectedUpdate] = []
    camera_events: list[CameraEventUpdate] = []

    camera.subscribe(MotionDetectedUpdate, lambda event: typed_events.append(event))
    camera.subscribe(CameraEventUpdate, lambda event: camera_events.append(event))

    await camera.handle_message(
        "cameras/TEST123/events/motion-detected",
        {
            "active_config": "primary",
            "duration": "10s",
            "file_duration": "10.000000s",
            "filename": "motion-2026-03-07_16-00-00.mp4",
            "level": "medium",
            "sensitivity": "0",
            "threshold": "40",
            "thumbnail": "motion-2026-03-07_16-00-00.jpeg",
            "timestamp": "2026-03-07T16:00:00Z",
        },
    )

    assert len(typed_events) == 1
    assert len(camera_events) == 1
    payload = typed_events[0].payload
    assert typed_events[0].event_type is EventType.MOTION_DETECTION
    assert payload.timestamp == "2026-03-07T16:00:00Z"
    assert payload.filename == "motion-2026-03-07_16-00-00.mp4"
    assert payload.active_config == "primary"
    assert payload.sensitivity == "0"
    assert payload.thumbnail is not None
    assert payload.thumbnail.endswith(".jpeg")


async def test_noise_detection_keeps_typed_payload() -> None:
    """A real sound-anomaly-detected payload should bind to the typed model."""

    camera = _create_camera()
    typed_events: list[NoiseDetectedUpdate] = []

    camera.subscribe(NoiseDetectedUpdate, lambda event: typed_events.append(event))

    await camera.handle_message(
        "cameras/TEST123/events/sound-anomaly-detected",
        REAL_NOISE_PAYLOAD,
    )

    assert len(typed_events) == 1
    payload = typed_events[0].payload
    assert typed_events[0].event_type is EventType.NOISE_DETECTION
    assert typed_events[0].event_key == "noise_detection"
    assert payload.active_config == "primary"
    assert payload.baseline == "-47.838640dB"
    assert payload.baseline_reference == "-47.838640dB"
    assert payload.file_duration == "10.000000s"
    assert payload.level == "-36.401137dB"
    assert payload.sensitivity == "0"
    assert payload.threshold == "-30.000000dB"
    assert payload.thumbnail is not None
    assert payload.thumbnail.endswith(".jpeg")
    assert payload.user_offset == "-30.000000dB"


async def test_noise_duration_is_not_parsed_as_a_hold() -> None:
    """The unit-suffixed duration is kept verbatim and drives no timer."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/events/sound-anomaly-detected",
        REAL_NOISE_PAYLOAD,
    )

    typed = camera.state.events["noise_detection"].last_payload
    assert typed["duration"] == "10s"
    # No task or handle may outlive the message: nothing schedules a reset.
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    assert pending == []


async def test_local_livekit_heartbeat_does_not_store_monitor_connected_state() -> None:
    """Local LiveKit heartbeats should not expose receiver presence as connectivity."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/events/local-livekit-heartbeat",
        {"receiver_present": True},
    )

    assert "monitor_connected" not in camera.state.values

    await camera.handle_message(
        "cameras/TEST123/events/local-livekit-heartbeat",
        {"receiver_present": False},
    )

    assert "monitor_connected" not in camera.state.values


async def test_default_camera_events_match_firmware_topics() -> None:
    """Only the two detections the firmware actually publishes should be seeded."""

    camera = _create_camera()

    assert set(camera.state.events) == {"motion_detection", "noise_detection"}


async def test_detection_events_record_last_seen_without_holding_state() -> None:
    """Detections are edge triggers: they stamp last_seen and nothing more."""

    camera = _create_camera()

    assert camera.state.events["noise_detection"].last_seen is None

    await camera.handle_message(
        "cameras/TEST123/events/sound-anomaly-detected",
        {
            "activeConfig": "primary",
            "duration": "90",
            "level": "loud",
            "threshold": "60",
            "timestamp": "2026-03-07T16:00:00Z",
            "filename": "anomaly-002.mp4",
        },
    )

    event_state = camera.state.events["noise_detection"]
    assert event_state.last_seen is not None
    assert event_state.topic == "cameras/TEST123/events/sound-anomaly-detected"
    # A long ``duration`` is the configured trigger threshold, not a hold time,
    # so it must not produce any lingering on/off state.
    assert not hasattr(event_state, "is_on")


async def test_repeated_detection_advances_last_seen() -> None:
    """Each detection should re-stamp last_seen so consumers can fire again."""

    camera = _create_camera()
    payload = {
        "activeConfig": "primary",
        "level": "medium",
        "threshold": "40",
        "timestamp": "2026-03-07T16:00:00Z",
        "filename": "motion-002.mp4",
    }

    await camera.handle_message("cameras/TEST123/events/motion-detected", payload)
    first = camera.state.events["motion_detection"].last_seen

    await camera.handle_message("cameras/TEST123/events/motion-detected", payload)
    second = camera.state.events["motion_detection"].last_seen

    assert first is not None
    assert second is not None
    assert second >= first


async def test_viewer_events_accept_nested_payloads() -> None:
    """Viewer join/left events should update immediately for nested payloads."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/events/viewer-joined",
        {
            "viewer": {
                "id": "viewer1",
                "identity": "viewer1",
                "client": "ios",
                "is_local": True,
                "role": "viewer",
            }
        },
    )

    assert camera.state.values["num_viewers"] == 1

    await camera.handle_message(
        "cameras/TEST123/events/viewer-left",
        {"viewer": {"id": "viewer1"}},
    )

    assert camera.state.values["num_viewers"] == 0


async def test_livekit_viewers_with_shared_user_id_count_by_identity() -> None:
    """App and monitor viewers can share a user id but are separate viewers."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/events/local-livekit-heartbeat",
        {
            "viewers_by_identity_full": {
                "app-device/users/user1": {
                    "client": "IOS_MOBILE",
                    "identity": "app-device/users/user1",
                    "is_local": True,
                    "role": "UNKNOWN",
                    "viewer_id": "user1",
                },
                "monitors/MONITOR123/users/user1": {
                    "client": "MONITOR",
                    "identity": "monitors/MONITOR123/users/user1",
                    "is_local": True,
                    "role": "UNKNOWN",
                    "viewer_id": "user1",
                },
            },
        },
    )

    assert camera.state.values["num_viewers"] == 2

    await camera.handle_message(
        "cameras/TEST123/events/viewer-left",
        {
            "client": "IOS_MOBILE",
            "identity": "app-device/users/user1",
            "viewer_id": "user1",
        },
    )

    assert camera.state.values["num_viewers"] == 1

    await camera.handle_message(
        "cameras/TEST123/events/viewer-joined",
        {
            "client": "IOS_MOBILE",
            "identity": "app-device/users/user1",
            "viewer_id": "user1",
        },
    )

    assert camera.state.values["num_viewers"] == 2
