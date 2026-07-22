from __future__ import annotations

from harbor.config import HarborCameraConfig
from harbor.devices.camera import SPEAKER_STATES, STREAM_QUALITIES, HarborCamera


def _create_camera() -> HarborCamera:
    config = HarborCameraConfig(
        serial="TEST123",
        cert_path="/path/to/cert.pem",
        key_path="/path/to/key.pem",
    )
    return HarborCamera(config)


async def test_enum_state_values_are_normalized_to_lowercase() -> None:
    """Device-reported enum values arrive upper case and must be lowercased."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/events/local-livekit-heartbeat",
        {"speaker_state": "PLAYING", "stream_quality": "GOOD"},
    )

    assert camera.state.values["speaker_state"] == "playing"
    assert camera.state.values["stream_quality"] == "good"
    assert camera.state.values["speaker_state"] in SPEAKER_STATES
    assert camera.state.values["stream_quality"] in STREAM_QUALITIES


async def test_unexpected_enum_value_maps_to_unknown() -> None:
    """An unrecognized enum value is stored as the in-set "unknown" member."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/events/local-livekit-heartbeat",
        {"speaker_state": "Buffering", "stream_quality": "EXCELLENT"},
    )

    assert camera.state.values["speaker_state"] == "unknown"
    assert camera.state.values["speaker_state"] in SPEAKER_STATES
    assert camera.state.values["stream_quality"] == "excellent"


async def test_unexpected_enum_value_replaces_prior_valid_value() -> None:
    """A later unrecognized value must not leave a stale valid value in place."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/events/local-livekit-heartbeat",
        {"stream_quality": "GOOD"},
    )
    assert camera.state.values["stream_quality"] == "good"

    await camera.handle_message(
        "cameras/TEST123/events/local-livekit-heartbeat",
        {"stream_quality": "DEGRADED"},
    )
    assert camera.state.values["stream_quality"] == "unknown"


async def test_missing_enum_values_do_not_clear_state() -> None:
    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/events/local-livekit-heartbeat",
        {"speaker_state": "IDLE", "stream_quality": "POOR"},
    )
    await camera.handle_message(
        "cameras/TEST123/events/local-livekit-heartbeat",
        {"bitrate": 1000.0},
    )

    assert camera.state.values["speaker_state"] == "idle"
    assert camera.state.values["stream_quality"] == "poor"


async def test_settings_update_maps_camera_control_state() -> None:
    """Camera settings should expose normalized stream and night-mode state."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/responses/get-settings",
        {
            "settings": {"preference_stream_paused": True},
            "state": {"video_night_mode": False},
        },
    )

    assert camera.state.values["camera_on"] is False
    assert camera.state.values["night_mode"] is False


async def test_missing_camera_settings_do_not_clear_control_state() -> None:
    """Partial settings responses should preserve previously known controls."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/responses/get-settings",
        {
            "settings": {"preference_stream_paused": False},
            "state": {"video_night_mode": True},
        },
    )
    await camera.handle_message(
        "cameras/TEST123/responses/get-settings",
        {"settings": {"preference_display_name": "Nursery"}},
    )

    assert camera.state.values["camera_on"] is True
    assert camera.state.values["night_mode"] is True
