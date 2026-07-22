from __future__ import annotations

import asyncio
import json
from typing import Any, cast
from unittest.mock import AsyncMock, patch

from harbor.config import HarborCameraConfig
from harbor.data.mqtt_models import Settings, SettingsEvent, SettingsState
from harbor.events import HarborEvent
from harbor.exceptions import HarborCommandError
from harbor.mqtt import (
    GET_SETTINGS_COMMAND,
    PAUSE_STREAM_COMMAND,
    UNPAUSE_STREAM_COMMAND,
    UPDATE_NIGHT_MODE_COMMAND,
    HarborMQTTClient,
)


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


async def test_message_handler_may_return_event() -> None:
    """MQTT handlers may return parsed events; the client ignores the value."""

    called = False

    async def message_handler(topic: str, payload: object) -> HarborEvent | None:
        nonlocal called
        called = True
        return None

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=message_handler,
    )

    await client._handle_message("test/topic", "{}")

    assert called is True


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


class _FakePublishClient:
    def __init__(self) -> None:
        self.published: list[tuple[str, str, int, bool]] = []

    async def publish(self, topic: str, payload: str, *, qos: int, retain: bool) -> None:
        self.published.append((topic, payload, qos, retain))


async def test_request_command_publishes_and_waits_for_matching_response() -> None:
    """Requests should publish to camera commands and resolve from response seq."""

    messages: list[tuple[str, object]] = []

    async def message_handler(topic: str, payload: object) -> None:
        messages.append((topic, payload))

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=message_handler,
    )
    fake_client = _FakePublishClient()
    client.connected = True
    client._client = cast(Any, fake_client)

    task = asyncio.create_task(
        client.request_command(
            "get-settings",
            {"seq": "seq-1", "client": "test-client", "triggeredBy": "harbor-python"},
            seq="seq-1",
            timeout=1,
        )
    )
    await asyncio.sleep(0)

    assert fake_client.published == [
        (
            "cameras/TEST123/get-settings",
            '{"seq":"seq-1","client":"test-client","triggeredBy":"harbor-python"}',
            2,
            False,
        )
    ]

    response = {
        "seq": "seq-1",
        "client": "test-client",
        "isUpdating": False,
        "settings": {"preference_display_name": "Nursery"},
    }
    await client._handle_message("cameras/TEST123/responses/get-settings", json.dumps(response))

    assert await task == response
    assert messages == [("cameras/TEST123/responses/get-settings", response)]


async def test_get_settings_uses_app_payload_shape() -> None:
    """The get-settings helper should use the APK's command topic and field names."""

    async def message_handler(topic: str, payload: object) -> None:
        pass

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=message_handler,
    )
    fake_client = _FakePublishClient()
    client.connected = True
    client._client = cast(Any, fake_client)

    task = asyncio.create_task(client.get_settings(client="test-client", triggered_by="users/user1", timeout=1))
    await asyncio.sleep(0)

    topic, payload_raw, qos, retain = fake_client.published[0]
    payload = json.loads(payload_raw)
    assert topic == "cameras/TEST123/get-settings"
    assert payload["client"] == "test-client"
    assert payload["triggeredBy"] == "users/user1"
    assert isinstance(payload["seq"], str)
    assert qos == 2
    assert retain is False

    await client._handle_message(
        "cameras/TEST123/responses/get-settings",
        json.dumps(
            {
                "seq": payload["seq"],
                "client": "test-client",
                "triggeredBy": "users/user1",
                "isUpdating": False,
                "settings": {"preference_display_name": "Nursery"},
            }
        ),
    )

    settings = await task
    assert settings.seq == payload["seq"]
    assert settings.triggered_by == "users/user1"
    assert settings.is_updating is False
    assert settings.settings is not None
    assert settings.settings.preference_display_name == "Nursery"


async def test_set_camera_on_runs_protocol_command_and_refreshes_settings() -> None:
    """Camera control should hide command details and return current settings."""

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=_noop_handler,
        client_id="test-client",
    )
    refreshed_settings = SettingsEvent(settings=Settings(preference_stream_paused=False))

    with (
        patch.object(
            client,
            "request_command",
            AsyncMock(return_value={"status": "OK"}),
        ) as request_command,
        patch.object(
            client,
            "get_settings",
            AsyncMock(return_value=refreshed_settings),
        ) as get_settings,
    ):
        await client.set_camera_on(
            True,
            viewer_id="home-assistant",
            timeout=3,
        )

    request_command.assert_awaited_once_with(
        UNPAUSE_STREAM_COMMAND,
        {"viewer_id": "home-assistant"},
        timeout=3,
    )
    get_settings.assert_awaited_once_with(timeout=3)


async def test_set_camera_off_uses_default_viewer_id() -> None:
    """Camera control should provide a stable viewer ID when none is supplied."""

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=_noop_handler,
        client_id="test-client",
    )

    with (
        patch.object(
            client,
            "request_command",
            AsyncMock(return_value={"message": "stream paused"}),
        ) as request_command,
        patch.object(
            client,
            "get_settings",
            AsyncMock(return_value=SettingsEvent()),
        ),
    ):
        await client.set_camera_on(False)

    request_command.assert_awaited_once_with(
        PAUSE_STREAM_COMMAND,
        {"viewer_id": "test-client"},
        timeout=10.0,
    )


async def test_set_night_mode_runs_protocol_command_and_refreshes_settings() -> None:
    """Night-mode control should hide command details and refresh state."""

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=_noop_handler,
    )
    refreshed_settings = SettingsEvent(state=SettingsState(video_night_mode=True))

    with (
        patch.object(
            client,
            "request_command",
            AsyncMock(return_value={"status": "OK"}),
        ) as request_command,
        patch.object(
            client,
            "get_settings",
            AsyncMock(return_value=refreshed_settings),
        ) as get_settings,
    ):
        await client.set_night_mode(True, timeout=4)

    request_command.assert_awaited_once_with(
        UPDATE_NIGHT_MODE_COMMAND,
        {"night_mode": True},
        timeout=4,
    )
    get_settings.assert_awaited_once_with(timeout=4)


async def test_settings_refresh_failure_does_not_mask_successful_command() -> None:
    """A post-command refresh failure should not report command failure."""

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=_noop_handler,
    )

    with (
        patch.object(
            client,
            "request_command",
            AsyncMock(return_value={"status": "OK"}),
        ),
        patch.object(
            client,
            "get_settings",
            AsyncMock(side_effect=TimeoutError),
        ),
    ):
        await client.set_night_mode(True)


async def test_camera_control_rejection_raises_library_error() -> None:
    """Rejected commands should not be delegated to library consumers."""

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=_noop_handler,
    )

    with (
        patch.object(
            client,
            "request_command",
            AsyncMock(return_value={"status": "ERROR", "error": "not allowed"}),
        ),
        patch.object(client, "get_settings", AsyncMock()) as get_settings,
    ):
        try:
            await client.set_night_mode(True)
        except HarborCommandError as err:
            assert err.command == UPDATE_NIGHT_MODE_COMMAND
            assert err.response == {"status": "ERROR", "error": "not allowed"}
        else:
            raise AssertionError("Expected HarborCommandError")

    get_settings.assert_not_awaited()


async def test_initial_commands_publish_get_settings_without_waiting() -> None:
    """Initial populate commands should request settings after connection."""

    async def message_handler(topic: str, payload: object) -> None:
        pass

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=message_handler,
        client_id="test-client",
        initial_commands=[GET_SETTINGS_COMMAND],
    )
    fake_client = _FakePublishClient()
    client.connected = True
    client._client = cast(Any, fake_client)

    await client._publish_initial_commands()

    assert len(fake_client.published) == 1
    topic, payload_raw, qos, retain = fake_client.published[0]
    payload = json.loads(payload_raw)
    assert topic == "cameras/TEST123/get-settings"
    assert payload["client"] == "test-client"
    assert payload["triggeredBy"] == "harbor-python"
    assert isinstance(payload["seq"], str)
    assert qos == 2
    assert retain is False
