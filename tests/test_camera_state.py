from __future__ import annotations

from harbor.config import HarborCameraConfig
from harbor.devices.camera import (
    NIGHT_MODE_PREFERENCES,
    SPEAKER_STATES,
    STREAM_QUALITIES,
    HarborCamera,
)


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


async def test_night_mode_preference_and_runtime_state_are_separate_keys() -> None:
    """The writable preference and the runtime observation must not collide.

    Under the "auto" preference the camera decides for itself whether IR is
    engaged, so a consumer reading back what it wrote needs the preference,
    not the runtime bool.
    """

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/responses/get-settings",
        {
            "settings": {"preference_video_night_mode": "auto"},
            "state": {"video_night_mode": False},
        },
    )

    assert camera.state.values["night_mode_preference"] == "auto"
    assert camera.state.values["night_mode"] is False


async def test_night_mode_preference_tracks_written_value() -> None:
    """Writing "on" must be readable back even before IR actually engages."""

    camera = _create_camera()

    for mode in ("auto", "on", "off"):
        await camera.handle_message(
            "cameras/TEST123/responses/get-settings",
            {"settings": {"preference_video_night_mode": mode}},
        )
        assert camera.state.values["night_mode_preference"] == mode
        assert camera.state.values["night_mode_preference"] in NIGHT_MODE_PREFERENCES


async def test_boolean_settings_are_exposed_as_state_values() -> None:
    """Image flip and clock overlay should read back as plain booleans."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/responses/get-settings",
        {"settings": {"preference_video_flip": True, "preference_video_has_clock_display": False}},
    )

    assert camera.state.values["video_flip"] is True
    assert camera.state.values["clock_display"] is False

    await camera.handle_message(
        "cameras/TEST123/responses/get-settings",
        {"settings": {"preference_video_flip": False, "preference_video_has_clock_display": True}},
    )

    assert camera.state.values["video_flip"] is False
    assert camera.state.values["clock_display"] is True


async def test_missing_boolean_settings_do_not_clear_state() -> None:
    """A partial settings payload must not drop known switch state."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/responses/get-settings",
        {"settings": {"preference_video_flip": True, "preference_video_has_clock_display": True}},
    )
    await camera.handle_message(
        "cameras/TEST123/responses/get-settings",
        {"settings": {"preference_display_name": "Nursery"}},
    )

    assert camera.state.values["video_flip"] is True
    assert camera.state.values["clock_display"] is True


async def test_choice_settings_preserve_case_in_state() -> None:
    """Verbatim-matched settings must read back exactly as they are written."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/responses/get-settings",
        {"settings": {"preference_temperature_scale": "F"}},
    )

    # "F", not "f" -- writing back a lowercased value would be rejected.
    assert camera.state.values["temperature_scale"] == "F"

    await camera.handle_message(
        "cameras/TEST123/responses/get-settings",
        {"settings": {"preference_temperature_scale": "C"}},
    )
    assert camera.state.values["temperature_scale"] == "C"


async def test_unexpected_choice_settings_map_to_unknown() -> None:
    """Values outside the firmware option list are clamped, not stored raw."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/responses/get-settings",
        {"settings": {"preference_temperature_scale": "K"}},
    )

    assert camera.state.values["temperature_scale"] == "unknown"


async def test_unexpected_night_mode_preference_maps_to_unknown() -> None:
    """An unrecognized preference is clamped like other enum state values."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/responses/get-settings",
        {"settings": {"preference_video_night_mode": "SCHEDULED"}},
    )

    assert camera.state.values["night_mode_preference"] == "unknown"


async def test_missing_camera_settings_do_not_clear_control_state() -> None:
    """Partial settings responses should preserve previously known controls."""

    camera = _create_camera()

    await camera.handle_message(
        "cameras/TEST123/responses/get-settings",
        {
            "settings": {"preference_stream_paused": False, "preference_video_night_mode": "on"},
            "state": {"video_night_mode": True},
        },
    )
    await camera.handle_message(
        "cameras/TEST123/responses/get-settings",
        {"settings": {"preference_display_name": "Nursery"}},
    )

    assert camera.state.values["camera_on"] is True
    assert camera.state.values["night_mode"] is True
    assert camera.state.values["night_mode_preference"] == "on"
