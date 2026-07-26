from __future__ import annotations

from unittest.mock import AsyncMock, patch

from harbor.config import HarborCameraConfig
from harbor.core import Harbor
from harbor.mqtt import DEFAULT_INITIAL_COMMANDS


def test_camera_connection_populates_settings_on_connect() -> None:
    """Camera connections should request initial state after connecting."""

    harbor = Harbor()
    config = HarborCameraConfig(
        serial="TEST123",
        cert_path="/path/to/cert.pem",
        key_path="/path/to/key.pem",
    )

    harbor.add_camera_connection(config)

    assert harbor._clients["TEST123"].initial_commands == DEFAULT_INITIAL_COMMANDS


async def test_camera_control_helpers_delegate_to_camera_client() -> None:
    """The high-level API should expose camera controls by serial number."""

    harbor = Harbor()
    config = HarborCameraConfig(
        serial="TEST123",
        cert_path="/path/to/cert.pem",
        key_path="/path/to/key.pem",
    )
    harbor.add_camera_connection(config)
    client = harbor._clients["TEST123"]
    with (
        patch.object(
            client,
            "set_camera_on",
            AsyncMock(),
        ) as set_camera_on,
        patch.object(
            client,
            "set_night_mode",
            AsyncMock(),
        ) as set_night_mode,
    ):
        await harbor.set_camera_on(
            "TEST123",
            False,
            viewer_id="home-assistant",
            timeout=3,
        )
        await harbor.set_night_mode("TEST123", "auto", timeout=4)

    set_camera_on.assert_awaited_once_with(
        False,
        viewer_id="home-assistant",
        timeout=3,
    )
    set_night_mode.assert_awaited_once_with("auto", timeout=4)


async def test_boolean_control_helpers_delegate_to_camera_client() -> None:
    """Switch-style controls should be reachable by serial number."""

    harbor = Harbor()
    config = HarborCameraConfig(
        serial="TEST123",
        cert_path="/path/to/cert.pem",
        key_path="/path/to/key.pem",
    )
    harbor.add_camera_connection(config)
    client = harbor._clients["TEST123"]

    with (
        patch.object(client, "set_video_flip", AsyncMock()) as set_video_flip,
        patch.object(client, "set_clock_display", AsyncMock()) as set_clock_display,
    ):
        await harbor.set_video_flip("TEST123", True, timeout=3)
        await harbor.set_clock_display("TEST123", False)

    set_video_flip.assert_awaited_once_with(True, timeout=3)
    set_clock_display.assert_awaited_once_with(False, timeout=10.0)


async def test_choice_control_helpers_delegate_to_camera_client() -> None:
    """Select-style controls should be reachable by serial number."""

    harbor = Harbor()
    config = HarborCameraConfig(
        serial="TEST123",
        cert_path="/path/to/cert.pem",
        key_path="/path/to/key.pem",
    )
    harbor.add_camera_connection(config)
    client = harbor._clients["TEST123"]

    with patch.object(client, "set_temperature_scale", AsyncMock()) as set_temperature_scale:
        await harbor.set_temperature_scale("TEST123", "C", timeout=3)

    set_temperature_scale.assert_awaited_once_with("C", timeout=3)


async def test_update_camera_settings_delegates_to_camera_client() -> None:
    """Arbitrary preference writes should be reachable by serial number."""

    harbor = Harbor()
    config = HarborCameraConfig(
        serial="TEST123",
        cert_path="/path/to/cert.pem",
        key_path="/path/to/key.pem",
    )
    harbor.add_camera_connection(config)
    client = harbor._clients["TEST123"]

    with patch.object(client, "update_settings", AsyncMock()) as update_settings:
        await harbor.update_camera_settings(
            "TEST123",
            {"preference_video_ir_brightness": 40},
            timeout=5,
        )

    update_settings.assert_awaited_once_with(
        {"preference_video_ir_brightness": 40},
        timeout=5,
    )
