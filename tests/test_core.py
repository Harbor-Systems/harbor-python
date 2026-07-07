from __future__ import annotations

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
