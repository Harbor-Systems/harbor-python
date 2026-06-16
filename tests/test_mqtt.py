from __future__ import annotations

from harbor.config import HarborCameraConfig
from harbor.mqtt import HarborMQTTClient


def _create_config() -> HarborCameraConfig:
    """Create a test camera config."""

    return HarborCameraConfig(
        serial="TEST123",
        cert_path="/path/to/cert.pem",
        key_path="/path/to/key.pem",
        cert_dir="/path/to/cert_dir",
        ip_address="192.168.1.100",
    )


def test_client_creation() -> None:
    """The MQTT client should keep the provided config."""

    async def message_handler(topic: str, payload: object) -> None:
        pass

    config = _create_config()
    client = HarborMQTTClient(config=config, topics=[], message_handler=message_handler)

    assert client.config.serial == "TEST123"
    assert client.config.ip_address == "192.168.1.100"


async def test_message_handler_receives_parsed_json() -> None:
    """Incoming JSON payloads should be decoded before dispatch."""

    messages: list[tuple[str, object]] = []

    async def message_handler(topic: str, payload: object) -> None:
        messages.append((topic, payload))

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=message_handler,
    )

    await client._handle_message("test/topic", '{"test": "data"}')

    assert messages == [("test/topic", {"test": "data"})]
