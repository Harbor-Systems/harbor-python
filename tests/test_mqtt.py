from __future__ import annotations

import asyncio

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


async def _noop_handler(topic: str, payload: object) -> None:
    pass


def _create_debounce_client(changes: list[bool], grace: float) -> HarborMQTTClient:
    """Create a client that records connection-change callbacks."""

    async def on_change(connected: bool) -> None:
        changes.append(connected)

    return HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=_noop_handler,
        on_connection_change=on_change,
        connection_grace_period=grace,
    )


async def test_connection_change_fires_on_first_connect() -> None:
    changes: list[bool] = []
    client = _create_debounce_client(changes, grace=0.1)

    await client._set_connected(True)

    assert changes == [True]
    assert client.connected is True


async def test_connection_change_suppresses_flapping() -> None:
    """A disconnect followed by a reconnect within the grace window is silent."""

    changes: list[bool] = []
    client = _create_debounce_client(changes, grace=0.1)

    await client._set_connected(True)
    await client._set_connected(False)
    await asyncio.sleep(0.02)
    await client._set_connected(True)
    await asyncio.sleep(0.2)

    assert changes == [True]


async def test_connection_change_reports_stable_disconnect() -> None:
    changes: list[bool] = []
    client = _create_debounce_client(changes, grace=0.05)

    await client._set_connected(True)
    await client._set_connected(False)
    await asyncio.sleep(0.15)

    assert changes == [True, False]
    assert client.connected is False


async def test_connection_change_zero_grace_reports_immediately() -> None:
    changes: list[bool] = []
    client = _create_debounce_client(changes, grace=0)

    await client._set_connected(True)
    await client._set_connected(False)

    assert changes == [True, False]


async def test_disconnect_before_first_connect_is_not_reported() -> None:
    changes: list[bool] = []
    client = _create_debounce_client(changes, grace=0)

    client.connected = True  # raw flag only; never reported as connected
    await client._set_connected(False)

    assert changes == []


async def test_stop_flushes_pending_disconnect() -> None:
    """An intentional stop should report the disconnect without waiting."""

    changes: list[bool] = []
    client = _create_debounce_client(changes, grace=60)

    await client._set_connected(True)
    await client._set_connected(False)
    await client.stop()

    assert changes == [True, False]
