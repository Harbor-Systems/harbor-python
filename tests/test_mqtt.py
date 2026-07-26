from __future__ import annotations

import asyncio
import json
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest

from harbor.config import HarborCameraConfig
from harbor.data.mqtt_models import Settings, SettingsEvent
from harbor.events import HarborEvent
from harbor.exceptions import HarborCommandError, HarborUnsupportedCommandError
from harbor.mqtt import (
    CLOCK_DISPLAY_PREFERENCE_KEY,
    GET_SETTINGS_COMMAND,
    NIGHT_MODE_MODES,
    NIGHT_MODE_PREFERENCE_KEY,
    PAUSE_STREAM_COMMAND,
    TEMPERATURE_SCALE_PREFERENCE_KEY,
    UNPAUSE_STREAM_COMMAND,
    UPDATE_SETTINGS_COMMAND,
    VIDEO_FLIP_PREFERENCE_KEY,
    HarborMQTTClient,
    NightMode,
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


def _connected_client(
    client_id: str | None = "test-client",
) -> tuple[HarborMQTTClient, _FakePublishClient]:
    """Create a client wired to a fake transport that records publishes."""

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=_noop_handler,
        client_id=client_id,
    )
    fake_client = _FakePublishClient()
    client.connected = True
    client._client = cast(Any, fake_client)
    return client, fake_client


async def _respond(client: HarborMQTTClient, command: str, payload: dict) -> None:
    """Feed a camera response back in on the matching responses topic."""

    await client._handle_message(
        f"cameras/TEST123/responses/{command}",
        json.dumps(payload),
    )


async def test_set_camera_on_pins_topic_and_payload() -> None:
    """unpause-stream must reach the wire with the documented payload."""

    client, fake_client = _connected_client()

    task = asyncio.create_task(client.set_camera_on(True, viewer_id="home-assistant", timeout=1))
    await asyncio.sleep(0)

    topic, payload_raw, qos, retain = fake_client.published[0]
    payload = json.loads(payload_raw)
    assert topic == "cameras/TEST123/unpause-stream"
    assert payload["viewer_id"] == "home-assistant"
    assert isinstance(payload["seq"], str)
    assert qos == 2
    assert retain is False

    await _respond(client, "unpause-stream", {"seq": payload["seq"], "status": "OK"})
    with patch.object(client, "get_settings", AsyncMock(return_value=SettingsEvent())):
        await task


async def test_set_camera_off_pins_topic_and_payload() -> None:
    """pause-stream must reach the wire with the documented payload."""

    client, fake_client = _connected_client()

    task = asyncio.create_task(client.set_camera_on(False, timeout=1))
    await asyncio.sleep(0)

    topic, payload_raw, _, _ = fake_client.published[0]
    payload = json.loads(payload_raw)
    assert topic == "cameras/TEST123/pause-stream"
    assert payload["viewer_id"] == "test-client"

    await _respond(client, "pause-stream", {"seq": payload["seq"], "status": "OK"})
    with patch.object(client, "get_settings", AsyncMock(return_value=SettingsEvent())):
        await task


async def test_update_settings_pins_topic_and_payload() -> None:
    """update-settings must match the shape the Harbor app publishes."""

    client, fake_client = _connected_client()

    task = asyncio.create_task(
        client.update_settings(
            {NIGHT_MODE_PREFERENCE_KEY: "auto", "preference_video_ir_brightness": 18},
            client="home-assistant",
            triggered_by="users/user1",
            timeout=1,
        )
    )
    await asyncio.sleep(0)

    topic, payload_raw, qos, retain = fake_client.published[0]
    payload = json.loads(payload_raw)
    assert topic == "cameras/TEST123/update-settings"
    assert payload["settings"] == {
        NIGHT_MODE_PREFERENCE_KEY: "auto",
        "preference_video_ir_brightness": 18,
    }
    assert payload["client"] == "home-assistant"
    assert payload["triggeredBy"] == "users/user1"
    assert isinstance(payload["seq"], str)
    assert set(payload) == {"seq", "settings", "client", "triggeredBy"}
    assert qos == 2
    assert retain is False

    # Response echoes back only the applied subset, as the firmware does.
    await _respond(
        client,
        "update-settings",
        {"seq": payload["seq"], "status": "OK", "settings": {NIGHT_MODE_PREFERENCE_KEY: "auto"}},
    )
    with patch.object(client, "get_settings", AsyncMock(return_value=SettingsEvent())):
        await task


async def test_update_settings_response_seq_must_match() -> None:
    """A response carrying a different seq must not resolve the request."""

    client, fake_client = _connected_client()

    task = asyncio.create_task(client.set_night_mode("on", timeout=0.15))
    await asyncio.sleep(0)
    payload = json.loads(fake_client.published[0][1])

    await _respond(client, "update-settings", {"seq": "some-other-seq", "status": "OK"})

    with pytest.raises(TimeoutError):
        await task
    assert payload["seq"] != "some-other-seq"


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
        seq=None,
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
        seq=None,
        timeout=10.0,
    )


async def test_set_night_mode_runs_protocol_command_and_refreshes_settings() -> None:
    """Night-mode control should write the preference and refresh state."""

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
            AsyncMock(return_value={"status": "OK"}),
        ) as request_command,
        patch.object(
            client,
            "get_settings",
            AsyncMock(return_value=SettingsEvent()),
        ) as get_settings,
    ):
        await client.set_night_mode("on", timeout=4)

    assert request_command.await_args is not None
    command, payload = request_command.await_args.args
    assert command == UPDATE_SETTINGS_COMMAND
    assert payload["settings"] == {NIGHT_MODE_PREFERENCE_KEY: "on"}
    assert payload["client"] == "test-client"
    assert payload["triggeredBy"] == "harbor-python"
    get_settings.assert_awaited_once_with(timeout=4)


@pytest.mark.parametrize("mode", NIGHT_MODE_MODES)
async def test_set_night_mode_accepts_every_supported_mode(mode: NightMode) -> None:
    """All three firmware-accepted modes should reach the wire verbatim."""

    client, fake_client = _connected_client()

    task = asyncio.create_task(client.set_night_mode(mode, timeout=1))
    await asyncio.sleep(0)

    topic, payload_raw, _, _ = fake_client.published[0]
    payload = json.loads(payload_raw)
    assert topic == "cameras/TEST123/update-settings"
    assert payload["settings"] == {NIGHT_MODE_PREFERENCE_KEY: mode}

    await _respond(client, "update-settings", {"seq": payload["seq"], "status": "OK"})
    with patch.object(client, "get_settings", AsyncMock(return_value=SettingsEvent())):
        await task


@pytest.mark.parametrize("mode", [True, False, "ON", "enabled", None, 1])
async def test_set_night_mode_rejects_non_enum_values(mode: object) -> None:
    """Booleans must not be silently coerced into a string mode."""

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=_noop_handler,
    )

    with patch.object(client, "request_command", AsyncMock()) as request_command:
        with pytest.raises(ValueError, match="night_mode must be one of"):
            await client.set_night_mode(mode)  # type: ignore[arg-type]

    request_command.assert_not_awaited()


@pytest.mark.parametrize(
    ("method", "key", "value"),
    [
        ("set_temperature_scale", TEMPERATURE_SCALE_PREFERENCE_KEY, "F"),
        ("set_temperature_scale", TEMPERATURE_SCALE_PREFERENCE_KEY, "C"),
    ],
)
async def test_choice_setting_pins_topic_and_payload(method: str, key: str, value: str) -> None:
    """Enum settings reach the wire verbatim, case included."""

    client, fake_client = _connected_client()

    task = asyncio.create_task(getattr(client, method)(value, timeout=1))
    await asyncio.sleep(0)

    topic, payload_raw, _, _ = fake_client.published[0]
    payload = json.loads(payload_raw)
    assert topic == "cameras/TEST123/update-settings"
    assert payload["settings"] == {key: value}

    await _respond(client, "update-settings", {"seq": payload["seq"], "status": "OK"})
    with patch.object(client, "get_settings", AsyncMock(return_value=SettingsEvent())):
        await task


@pytest.mark.parametrize(
    ("method", "value"),
    [
        # Temperature scale is matched verbatim, so case matters.
        ("set_temperature_scale", "f"),
        ("set_temperature_scale", "c"),
        ("set_temperature_scale", "celsius"),
        ("set_temperature_scale", True),
        ("set_temperature_scale", None),
    ],
)
async def test_choice_setting_rejects_unknown_value(method: str, value: object) -> None:
    """Values outside the firmware's option list never reach the wire."""

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=_noop_handler,
    )

    with patch.object(client, "request_command", AsyncMock()) as request_command:
        with pytest.raises(ValueError, match="must be one of"):
            await getattr(client, method)(value)

    request_command.assert_not_awaited()


@pytest.mark.parametrize(
    ("method", "key"),
    [
        ("set_video_flip", VIDEO_FLIP_PREFERENCE_KEY),
        ("set_clock_display", CLOCK_DISPLAY_PREFERENCE_KEY),
    ],
)
@pytest.mark.parametrize("value", [True, False])
async def test_boolean_setting_pins_topic_and_payload(method: str, key: str, value: bool) -> None:
    """Boolean settings are written as JSON booleans on the update-settings topic."""

    client, fake_client = _connected_client()

    task = asyncio.create_task(getattr(client, method)(value, timeout=1))
    await asyncio.sleep(0)

    topic, payload_raw, qos, retain = fake_client.published[0]
    payload = json.loads(payload_raw)
    assert topic == "cameras/TEST123/update-settings"
    assert payload["settings"] == {key: value}
    # A JSON bool, not 1/0 -- the firmware types these as boolean.
    assert f'"{key}":{"true" if value else "false"}' in payload_raw
    assert set(payload) == {"seq", "settings", "client", "triggeredBy"}
    assert qos == 2
    assert retain is False

    await _respond(client, "update-settings", {"seq": payload["seq"], "status": "OK"})
    with patch.object(client, "get_settings", AsyncMock(return_value=SettingsEvent())):
        await task


@pytest.mark.parametrize("method", ["set_video_flip", "set_clock_display"])
@pytest.mark.parametrize("value", [1, 0, "true", "on", None])
async def test_boolean_setting_rejects_non_bool(method: str, value: object) -> None:
    """Truthy stand-ins must not be sent as numbers or strings."""

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=_noop_handler,
    )

    with patch.object(client, "request_command", AsyncMock()) as request_command:
        with pytest.raises(ValueError, match="must be a bool"):
            await getattr(client, method)(value)

    request_command.assert_not_awaited()


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
        await client.set_night_mode("auto")


async def test_camera_control_rejection_raises_library_error() -> None:
    """Rejected commands should not be delegated to library consumers."""

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=_noop_handler,
    )
    response = {"status": "ERROR", "error": "not allowed"}

    with (
        patch.object(client, "request_command", AsyncMock(return_value=response)),
        patch.object(client, "get_settings", AsyncMock()) as get_settings,
    ):
        with pytest.raises(HarborCommandError) as excinfo:
            await client.set_night_mode("on")

    assert excinfo.value.command == UPDATE_SETTINGS_COMMAND
    assert excinfo.value.response == response
    assert excinfo.value.status == "ERROR"
    assert not isinstance(excinfo.value, HarborUnsupportedCommandError)
    get_settings.assert_not_awaited()


async def test_unsupported_command_raises_distinct_error() -> None:
    """RESOURCE_NOT_FOUND is permanent and must be distinguishable."""

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=_noop_handler,
    )
    response = {"seq": "seq-1", "status": "RESOURCE_NOT_FOUND"}

    with (
        patch.object(client, "request_command", AsyncMock(return_value=response)),
        patch.object(client, "get_settings", AsyncMock()) as get_settings,
    ):
        with pytest.raises(HarborUnsupportedCommandError) as excinfo:
            await client.set_night_mode("on")

    # Still a HarborCommandError, so existing handlers keep working.
    assert isinstance(excinfo.value, HarborCommandError)
    assert excinfo.value.status == "RESOURCE_NOT_FOUND"
    assert excinfo.value.command == UPDATE_SETTINGS_COMMAND
    get_settings.assert_not_awaited()


async def test_invalid_setting_value_surfaces_field_errors() -> None:
    """The firmware's per-field errors array should reach the caller parsed."""

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=_noop_handler,
    )
    # Shape captured from firmware when writing an out-of-range value.
    response = {
        "status": "REQUEST_MALFORMED",
        "errors": [
            {
                "error_code": "INVALID_VALUE",
                "key": f"/{NIGHT_MODE_PREFERENCE_KEY}",
                "schema": {"default": "auto", "options": ["auto", "on", "off"], "type": "string"},
                "value": "bogus-mode",
            }
        ],
    }

    with (
        patch.object(client, "request_command", AsyncMock(return_value=response)),
        patch.object(client, "get_settings", AsyncMock()),
    ):
        with pytest.raises(HarborCommandError) as excinfo:
            await client.update_settings({NIGHT_MODE_PREFERENCE_KEY: "bogus-mode"})

    assert excinfo.value.status == "REQUEST_MALFORMED"
    assert excinfo.value.errors[0]["error_code"] == "INVALID_VALUE"
    assert not isinstance(excinfo.value, HarborUnsupportedCommandError)


async def test_error_without_status_still_raises_with_null_status() -> None:
    """A bare error payload has no status but is still a rejection."""

    client = HarborMQTTClient(
        config=_create_config(),
        topics=[],
        message_handler=_noop_handler,
    )

    with (
        patch.object(client, "request_command", AsyncMock(return_value={"error": "nope"})),
        patch.object(client, "get_settings", AsyncMock()),
    ):
        with pytest.raises(HarborCommandError) as excinfo:
            await client.set_night_mode("off")

    assert excinfo.value.status is None


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
