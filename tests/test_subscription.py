from __future__ import annotations

from harbor.config import HarborCameraConfig
from harbor.devices.camera import HarborCamera
from harbor.events import (
    CameraEventUpdate,
    EventType,
    HarborEvent,
    HarborEventBus,
    HeartbeatUpdate,
    MotionDetectedUpdate,
    ViewerJoinedUpdate,
    extract_explicit_event_state,
)


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


def test_event_bus_parses_generic_camera_events() -> None:
    """The package should normalize generic camera events."""

    events_received: list[CameraEventUpdate] = []
    event_bus = HarborEventBus()

    def on_camera_event(event: HarborEvent) -> None:
        assert isinstance(event, CameraEventUpdate)
        events_received.append(event)

    event_bus.subscribe(CameraEventUpdate, on_camera_event)

    async def _run() -> None:
        await event_bus.async_process_message(
            "cameras/TEST123/events/noise-detection",
            {"duration": "2", "detected": True},
        )

    import asyncio

    asyncio.run(_run())

    assert len(events_received) == 1
    assert events_received[0].event_key == "noise_detection"
    assert events_received[0].active_seconds == 2.0
    assert events_received[0].explicit_state is True


async def test_motion_detection_keeps_typed_payload() -> None:
    """Known trigger events should keep their typed payload and trigger base subscribers."""

    camera = _create_camera()
    typed_events: list[MotionDetectedUpdate] = []
    camera_events: list[CameraEventUpdate] = []

    camera.subscribe(MotionDetectedUpdate, lambda event: typed_events.append(event))
    camera.subscribe(CameraEventUpdate, lambda event: camera_events.append(event))

    await camera.handle_message(
        "cameras/TEST123/events/motion-detection",
        {"duration": "1", "timestamp": "2026-03-07T16:00:00Z"},
    )

    assert len(typed_events) == 1
    assert len(camera_events) == 1
    assert typed_events[0].event_type is EventType.MOTION_DETECTION
    assert typed_events[0].payload.timestamp == "2026-03-07T16:00:00Z"
    assert typed_events[0].active_seconds == 1.0


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


async def test_default_camera_events_include_noise_detection() -> None:
    """Noise detection should be initialized like motion and cry detection."""

    camera = _create_camera()

    assert set(camera.state.events) == {
        "cry_detection",
        "motion_detection",
        "noise_detection",
    }


async def test_detected_topic_aliases_trigger_camera_events() -> None:
    """Detected topic aliases should normalize to the HA entity keys."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/events/cry-detected",
        {"duration": "1", "detected": True},
    )
    await camera.handle_message(
        "cameras/TEST123/events/noise-detected",
        {"duration": "1", "detected": True},
    )

    assert camera.state.events["cry_detection"].is_on is True
    assert camera.state.events["noise_detection"].is_on is True


def test_detection_boolean_payloads_provide_explicit_state() -> None:
    """Detection-specific boolean payloads should be treated as explicit state."""

    assert extract_explicit_event_state({"motion_detected": False}) is False
    assert extract_explicit_event_state({"cry_detected": False}) is False
    assert extract_explicit_event_state({"noise_detected": False}) is False
    assert extract_explicit_event_state({"motion": True}) is True
    assert extract_explicit_event_state({"cry": True}) is True
    assert extract_explicit_event_state({"noise": True}) is True


async def test_explicit_false_detection_payload_keeps_event_off() -> None:
    """Explicit false detection payloads should not pulse the event on."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/events/noise-detected",
        {"noise_detected": False},
    )

    assert camera.state.events["noise_detection"].is_on is False


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
