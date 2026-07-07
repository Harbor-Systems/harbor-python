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


async def test_unexpected_enum_value_is_passed_through_lowercased() -> None:
    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/events/local-livekit-heartbeat",
        {"speaker_state": "Buffering", "stream_quality": "EXCELLENT"},
    )

    assert camera.state.values["speaker_state"] == "buffering"
    assert camera.state.values["stream_quality"] == "excellent"


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
