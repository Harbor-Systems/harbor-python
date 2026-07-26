from __future__ import annotations

import asyncio
import json
import logging
import sys
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

from aiomqtt import Client, MqttError

from .config import HarborCameraConfig
from .data.mqtt_models import (
    GetCameraSettingsRequest,
    SettingsEvent,
    UpdateCameraSettingsRequest,
)
from .exceptions import (
    RESOURCE_NOT_FOUND_STATUS,
    HarborCommandError,
    HarborUnsupportedCommandError,
)
from .utils import get_camera_host, get_ssl_cache_key, get_ssl_context

if TYPE_CHECKING:
    from .events import HarborEvent

_LOGGER = logging.getLogger(__name__)

#: Night mode is a three-way preference on the device, not an on/off switch.
NightMode = Literal["auto", "on", "off"]

#: Unit the camera reports temperatures in. Case-sensitive on the wire.
TemperatureScale = Literal["F", "C"]

DEFAULT_CONNECTION_GRACE_PERIOD = 90.0
DEFAULT_COMMAND_QOS = 2
DEFAULT_REQUEST_TIMEOUT = 10.0
GET_SETTINGS_COMMAND = "get-settings"
UPDATE_SETTINGS_COMMAND = "update-settings"
PAUSE_STREAM_COMMAND = "pause-stream"
UNPAUSE_STREAM_COMMAND = "unpause-stream"
DEFAULT_INITIAL_COMMANDS = (GET_SETTINGS_COMMAND,)

#: Settings key holding the writable night-mode preference.
NIGHT_MODE_PREFERENCE_KEY = "preference_video_night_mode"

#: Accepted night-mode values, as reported by the firmware itself: rejecting an
#: invalid value returns ``{"options": ["auto", "on", "off"], "default": "auto"}``.
#: Night mode is a three-way preference, not a boolean.
NIGHT_MODE_MODES: tuple[NightMode, ...] = ("auto", "on", "off")
DEFAULT_NIGHT_MODE: NightMode = "auto"

#: Settings key for rotating the video image 180 degrees. Genuinely a boolean:
#: the firmware reports ``{"default": false, "type": "boolean"}``.
VIDEO_FLIP_PREFERENCE_KEY = "preference_video_flip"

#: Settings key for the on-video clock overlay. Genuinely a boolean:
#: the firmware reports ``{"default": true, "type": "boolean"}``.
CLOCK_DISPLAY_PREFERENCE_KEY = "preference_video_has_clock_display"

#: Settings key for the temperature unit. Firmware schema:
#: ``{"default": "F", "options": ["F", "C"], "type": "string"}``.
TEMPERATURE_SCALE_PREFERENCE_KEY = "preference_temperature_scale"
TEMPERATURE_SCALES: tuple[TemperatureScale, ...] = ("F", "C")
DEFAULT_TEMPERATURE_SCALE: TemperatureScale = "F"


class HarborMQTTClient:
    def __init__(
        self,
        config: HarborCameraConfig,
        topics: list[str],
        message_handler: Callable[[str, Any], Awaitable[HarborEvent | None]],
        client_id: str | None = None,
        ssl_context_cache: dict | None = None,
        on_connection_change: Callable[[bool], Awaitable[None]] | None = None,
        connection_grace_period: float = DEFAULT_CONNECTION_GRACE_PERIOD,
        initial_commands: list[str] | tuple[str, ...] | None = None,
    ) -> None:
        """Initialize the MQTT client.

        ``on_connection_change`` fires only on stable connection-state
        transitions: a disconnect is reported only if the client stays
        disconnected for ``connection_grace_period`` seconds, so routine
        TCP flapping never reaches the listener. Set the grace period to 0
        to report every raw transition. ``connected`` always reflects the
        raw transport state.
        """
        self.config = config
        self.topics = topics
        self.message_handler = message_handler
        self.client_id = client_id
        self.ssl_context_cache = ssl_context_cache or {}
        self.on_connection_change = on_connection_change
        self.connection_grace_period = connection_grace_period
        self.initial_commands = tuple(initial_commands or ())
        self.connected: bool = False
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._client: Client | None = None
        self._pending_responses: dict[str, asyncio.Future[Any]] = {}
        self._reported_connected: bool | None = None
        self._disconnect_grace_task: asyncio.Task | None = None

    async def _handle_message(self, topic: str, payload_raw: str) -> None:
        try:
            payload = json.loads(payload_raw)
        except Exception:
            payload = payload_raw

        await self.message_handler(topic, payload)
        self._resolve_pending_response(topic, payload)

    def _resolve_pending_response(self, topic: str, payload: Any) -> None:
        """Resolve a pending request when a camera response echoes its seq."""
        if not topic.startswith(f"cameras/{self.config.serial}/responses/"):
            return
        if not isinstance(payload, dict):
            return

        seq = payload.get("seq")
        if not isinstance(seq, str):
            return

        future = self._pending_responses.pop(seq, None)
        if future is None or future.done():
            return
        future.set_result(payload)

    async def _set_connected(self, connected: bool) -> None:
        """Update the raw connection flag and debounce listener notifications."""
        if self.connected == connected:
            return
        self.connected = connected

        if connected:
            self._cancel_disconnect_grace()
            if self._reported_connected is not True:
                await self._notify_connection_change(True)
            return

        if self._reported_connected is not True:
            return
        if self.connection_grace_period <= 0:
            await self._notify_connection_change(False)
            return
        if self._disconnect_grace_task is None or self._disconnect_grace_task.done():
            self._disconnect_grace_task = asyncio.create_task(self._disconnect_after_grace())

    async def _disconnect_after_grace(self) -> None:
        """Report a disconnect only if it survives the grace period."""
        await asyncio.sleep(self.connection_grace_period)
        self._disconnect_grace_task = None
        if not self.connected:
            _LOGGER.info(
                "Harbor: camera %s still disconnected after %s second grace period",
                self.config.serial,
                self.connection_grace_period,
            )
            await self._notify_connection_change(False)

    def _cancel_disconnect_grace(self) -> None:
        if self._disconnect_grace_task is not None:
            self._disconnect_grace_task.cancel()
            self._disconnect_grace_task = None

    async def _notify_connection_change(self, connected: bool) -> None:
        self._reported_connected = connected
        if self.on_connection_change is None:
            return
        try:
            await self.on_connection_change(connected)
        except Exception:
            _LOGGER.exception(
                "Harbor: connection-change listener raised for camera %s",
                self.config.serial,
            )

    def _invalidate_ssl_cache(self) -> None:
        self.ssl_context_cache.pop(get_ssl_cache_key(self.config), None)

    def _fail_pending_responses(self, exc: Exception) -> None:
        for future in self._pending_responses.values():
            if not future.done():
                future.set_exception(exc)
        self._pending_responses.clear()

    async def run(self) -> None:
        reconnect_delay = 2

        _LOGGER.info("Harbor: MQTT client starting for camera %s", self.config.serial)

        try:
            while not self._stop_event.is_set():
                # Fetch the SSL context each attempt so invalidations in the
                # error handlers below take effect on the next reconnect;
                # unchanged material is a cheap cache hit.
                try:
                    loop = asyncio.get_running_loop()
                    ssl_ctx = await loop.run_in_executor(None, get_ssl_context, self.config, self.ssl_context_cache)
                except Exception as e:
                    _LOGGER.error("Harbor: Failed to create SSL context for camera %s: %s", self.config.serial, e)
                    # Ensure we clear any partial state
                    self._invalidate_ssl_cache()
                    return

                try:
                    host = get_camera_host(self.config)
                    _LOGGER.info(
                        "Harbor: MQTT attempting connection to %s:%s for camera %s",
                        host,
                        8884,
                        self.config.serial,
                    )

                    async with Client(
                        hostname=host,
                        port=8884,
                        tls_context=ssl_ctx,
                        timeout=10,
                        identifier=self.client_id,
                    ) as client:
                        self._client = client
                        _LOGGER.info(
                            "Harbor: MQTT connected to %s:%s for camera %s",
                            host,
                            8884,
                            self.config.serial,
                        )
                        await self._set_connected(True)
                        try:
                            if self.topics:
                                await client.subscribe([(t, 0) for t in self.topics])
                                _LOGGER.info(
                                    "Harbor: MQTT subscribed to topics: %s for camera %s",
                                    self.topics,
                                    self.config.serial,
                                )

                            await self._publish_initial_commands()

                            async for message in client.messages:
                                if self._stop_event.is_set():
                                    break

                                payload_raw = message.payload.decode("utf-8", errors="replace")
                                topic = str(message.topic)

                                _LOGGER.debug(
                                    "Harbor: MQTT message received on topic '%s' from camera %s: %s",
                                    topic,
                                    self.config.serial,
                                    payload_raw,
                                )

                                await self._handle_message(topic, payload_raw)

                            reconnect_delay = 2
                        finally:
                            self._client = None
                            self._fail_pending_responses(ConnectionError("Harbor MQTT client disconnected"))
                            await self._set_connected(False)

                except TimeoutError as e:
                    _LOGGER.warning("Harbor: MQTT connection timeout for %s: %s (reconnecting)", self.config.serial, e)
                    # Clear SSL context on timeout as it might be a stale session
                    self._invalidate_ssl_cache()

                except MqttError as e:
                    _LOGGER.warning("Harbor: MQTT error for %s: %s (reconnecting)", self.config.serial, e)
                    _LOGGER.info("Harbor: MQTT disconnected from camera %s", self.config.serial)
                    # Clear SSL context on MQTT error
                    self._invalidate_ssl_cache()

                except OSError as e:
                    _LOGGER.warning("Harbor: MQTT OS error for %s: %s (reconnecting)", self.config.serial, e)
                    # Critical to clear context here for WinError 10065 cleanup
                    self._invalidate_ssl_cache()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    _LOGGER.error("Harbor: MQTT unexpected error for %s: %s (reconnecting)", self.config.serial, e)
                    # Clear context on unexpected errors too
                    self._invalidate_ssl_cache()
                    import traceback

                    _LOGGER.error(traceback.format_exc())

                try:
                    _LOGGER.info(
                        "Harbor: MQTT waiting %s seconds before reconnecting to camera %s",
                        reconnect_delay,
                        self.config.serial,
                    )
                    await asyncio.wait_for(self._stop_event.wait(), timeout=reconnect_delay)
                except TimeoutError:
                    pass
                reconnect_delay = min(reconnect_delay * 2, 30)

        except asyncio.CancelledError:
            _LOGGER.info("Harbor: MQTT task cancelled for camera %s", self.config.serial)
            raise

    async def start(self) -> None:
        if sys.platform == "win32" and isinstance(asyncio.get_running_loop(), asyncio.ProactorEventLoop):
            _LOGGER.warning(
                "Harbor: You are running on Windows with the default ProactorEventLoop. "
                "This is known to cause issues with aiomqtt. "
                "Please use WindowsSelectorEventLoopPolicy instead: "
                "asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())"
            )

        if self._task and not self._task.done():
            _LOGGER.info("Harbor: MQTT client already running for camera %s", self.config.serial)
            return
        _LOGGER.info("Harbor: MQTT client starting for camera %s", self.config.serial)
        self._stop_event.clear()
        self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        _LOGGER.info("Harbor: MQTT client stopping for camera %s", self.config.serial)
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            _LOGGER.info("Harbor: MQTT client stopped for camera %s", self.config.serial)
        # An intentional stop is a stable disconnect: skip the grace period.
        self._cancel_disconnect_grace()
        self._client = None
        self._fail_pending_responses(ConnectionError("Harbor MQTT client stopped"))
        if self._reported_connected:
            await self._notify_connection_change(False)

    async def publish(
        self,
        topic: str,
        payload: Any,
        *,
        qos: int = DEFAULT_COMMAND_QOS,
        retain: bool = False,
    ) -> None:
        """Publish a JSON-compatible payload to a Harbor MQTT topic."""
        client = self._client
        if client is None or not self.connected:
            raise ConnectionError(f"Harbor MQTT client is not connected for camera {self.config.serial}")

        if isinstance(payload, str):
            payload_raw = payload
        else:
            payload_raw = json.dumps(payload, separators=(",", ":"))

        _LOGGER.debug(
            "Harbor: MQTT publishing to topic '%s' for camera %s: %s",
            topic,
            self.config.serial,
            payload_raw,
        )
        await client.publish(topic, payload_raw, qos=qos, retain=retain)

    async def publish_command(
        self,
        command: str,
        payload: Any,
        *,
        qos: int = DEFAULT_COMMAND_QOS,
    ) -> None:
        """Publish a camera command using the app's command topic layout."""
        await self.publish(f"cameras/{self.config.serial}/{command}", payload, qos=qos)

    async def _publish_initial_commands(self) -> None:
        """Publish configured one-shot state population commands after connect."""
        for command in self.initial_commands:
            try:
                if command == GET_SETTINGS_COMMAND:
                    await self.publish_command(command, self._build_get_settings_payload())
                else:
                    _LOGGER.warning(
                        "Harbor: skipping unsupported initial command %s for camera %s",
                        command,
                        self.config.serial,
                    )
            except Exception:
                _LOGGER.exception(
                    "Harbor: failed to publish initial command %s for camera %s",
                    command,
                    self.config.serial,
                )

    def _build_get_settings_payload(
        self,
        *,
        client: str | None = None,
        triggered_by: str | None = None,
        seq: str | None = None,
    ) -> dict[str, Any]:
        request = GetCameraSettingsRequest(
            seq=seq or _generate_seq(),
            client=client or self.client_id or f"harbor-client-{self.config.serial}",
            triggeredBy=triggered_by or "harbor-python",
        )
        return request.model_dump(by_alias=True)

    async def request_command(
        self,
        command: str,
        payload: dict[str, Any],
        *,
        seq: str | None = None,
        timeout: float = DEFAULT_REQUEST_TIMEOUT,
        qos: int = DEFAULT_COMMAND_QOS,
    ) -> Any:
        """Publish a command and wait for a response carrying the same seq."""
        request_seq = seq or _generate_seq()
        payload = {**payload, "seq": request_seq}
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending_responses[request_seq] = future

        try:
            await self.publish_command(command, payload, qos=qos)
            return await asyncio.wait_for(future, timeout=timeout)
        except Exception:
            pending = self._pending_responses.pop(request_seq, None)
            if pending is not None and not pending.done():
                pending.cancel()
            raise

    async def get_settings(
        self,
        *,
        client: str | None = None,
        triggered_by: str | None = None,
        timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ) -> SettingsEvent:
        """Request camera settings via the get-settings command."""
        seq = _generate_seq()
        response = await self.request_command(
            GET_SETTINGS_COMMAND,
            self._build_get_settings_payload(
                seq=seq,
                client=client,
                triggered_by=triggered_by,
            ),
            seq=seq,
            timeout=timeout,
        )
        return SettingsEvent.model_validate(response)

    async def set_camera_on(
        self,
        camera_on: bool,
        *,
        viewer_id: str | None = None,
        timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        """Turn the camera stream on or off and refresh its settings."""
        command = UNPAUSE_STREAM_COMMAND if camera_on else PAUSE_STREAM_COMMAND
        await self._request_camera_control(
            command,
            {"viewer_id": viewer_id or self.client_id or f"harbor-client-{self.config.serial}"},
            timeout=timeout,
        )
        await self._refresh_settings_after_command(command, timeout=timeout)

    async def set_night_mode(
        self,
        night_mode: NightMode,
        *,
        timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        """Set the camera night-mode preference and refresh its settings.

        ``night_mode`` is one of ``"auto"``, ``"on"`` or ``"off"`` — the
        device models night mode as a three-way preference, so booleans are
        rejected rather than guessed at. ``"auto"`` lets the camera engage IR
        on its own in low light; the resulting runtime state is reported
        separately as ``SettingsState.video_night_mode``.
        """
        _require_choice(
            "night_mode",
            night_mode,
            NIGHT_MODE_MODES,
            "Night mode is a three-way preference, not a boolean.",
        )
        await self.update_settings({NIGHT_MODE_PREFERENCE_KEY: night_mode}, timeout=timeout)

    async def set_temperature_scale(
        self,
        temperature_scale: TemperatureScale,
        *,
        timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        """Set the unit the camera reports temperatures in.

        ``temperature_scale`` is ``"F"`` or ``"C"``, upper case — the device
        matches the value exactly.
        """
        _require_choice("temperature_scale", temperature_scale, TEMPERATURE_SCALES)
        await self.update_settings(
            {TEMPERATURE_SCALE_PREFERENCE_KEY: temperature_scale},
            timeout=timeout,
        )

    async def set_video_flip(
        self,
        video_flip: bool,
        *,
        timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        """Rotate the camera image 180 degrees, or restore it upright."""
        await self.update_settings(
            {VIDEO_FLIP_PREFERENCE_KEY: _require_bool("video_flip", video_flip)},
            timeout=timeout,
        )

    async def set_clock_display(
        self,
        clock_display: bool,
        *,
        timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        """Show or hide the clock overlay burned into the video."""
        await self.update_settings(
            {CLOCK_DISPLAY_PREFERENCE_KEY: _require_bool("clock_display", clock_display)},
            timeout=timeout,
        )

    async def update_settings(
        self,
        settings: dict[str, Any],
        *,
        client: str | None = None,
        triggered_by: str | None = None,
        timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        """Write camera preferences via the update-settings command.

        ``settings`` maps preference keys (as they appear under ``settings``
        in a ``get-settings`` response) to their new values. The camera
        validates each key and returns a per-field ``errors`` array when a
        value is out of range, which is surfaced on
        :class:`~harbor.exceptions.HarborCommandError`.
        """
        request = UpdateCameraSettingsRequest(
            seq=_generate_seq(),
            settings=settings,
            client=client or self.client_id or f"harbor-client-{self.config.serial}",
            triggeredBy=triggered_by or "harbor-python",
        )
        payload = request.model_dump(by_alias=True)
        await self._request_camera_control(
            UPDATE_SETTINGS_COMMAND,
            payload,
            seq=payload["seq"],
            timeout=timeout,
        )
        await self._refresh_settings_after_command(
            UPDATE_SETTINGS_COMMAND,
            timeout=timeout,
        )

    async def _request_camera_control(
        self,
        command: str,
        payload: dict[str, Any],
        *,
        seq: str | None = None,
        timeout: float,
    ) -> Any:
        """Run a camera control command and reject error responses.

        A ``RESOURCE_NOT_FOUND`` status means this firmware build has no
        handler for the command, which no amount of retrying will fix, so it
        is raised as :class:`HarborUnsupportedCommandError` to let consumers
        skip the feature instead of failing on every use.
        """
        response = await self.request_command(command, payload, seq=seq, timeout=timeout)
        if not isinstance(response, dict):
            return response

        status = response.get("status")
        status_text = None if status is None else str(status).upper()
        if not (response.get("error") or response.get("errors") or (status_text is not None and status_text != "OK")):
            return response

        if status_text == RESOURCE_NOT_FOUND_STATUS:
            _LOGGER.warning(
                "Harbor: camera %s firmware does not support command %s",
                self.config.serial,
                command,
            )
            raise HarborUnsupportedCommandError(command, response)
        raise HarborCommandError(command, response)

    async def _refresh_settings_after_command(
        self,
        command: str,
        *,
        timeout: float,
    ) -> None:
        """Refresh settings without turning a successful command into a failure."""
        try:
            await self.get_settings(timeout=timeout)
        except (ConnectionError, MqttError, TimeoutError):
            _LOGGER.debug(
                "Unable to refresh settings after Harbor command %s for camera %s",
                command,
                self.config.serial,
                exc_info=True,
            )

    def __del__(self) -> None:
        if self._stop_event and not self._stop_event.is_set():
            self._stop_event.set()


def _generate_seq() -> str:
    """Generate a request sequence string echoed by Harbor responses."""
    return uuid4().hex


def _require_choice(name: str, value: Any, choices: tuple[str, ...], hint: str = "") -> str:
    """Reject a value outside the set the firmware accepts for a setting.

    Matching is exact: the device compares the string verbatim, so a
    differently-cased value would be rejected on the wire anyway.
    """
    if not isinstance(value, str) or value not in choices:
        message = f"{name} must be one of {choices!r}, got {value!r}"
        raise ValueError(f"{message}. {hint}" if hint else message)
    return value


def _require_bool(name: str, value: Any) -> bool:
    """Reject non-boolean input for a setting the device types as a boolean.

    ``1``/``0`` are ``int`` and would serialize as numbers, which the firmware
    rejects, so they are refused here with a clearer message.
    """
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a bool, got {value!r}")
    return value
