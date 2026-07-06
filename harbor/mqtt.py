import asyncio
import json
import logging
import sys
from collections.abc import Awaitable, Callable
from typing import Any

from aiomqtt import Client, MqttError

from .config import HarborCameraConfig
from .utils import get_camera_host, get_ssl_cache_key, get_ssl_context

_LOGGER = logging.getLogger(__name__)

DEFAULT_CONNECTION_GRACE_PERIOD = 90.0


class HarborMQTTClient:
    def __init__(
        self,
        config: HarborCameraConfig,
        topics: list[str],
        message_handler: Callable[[str, Any], Awaitable[None]],
        client_id: str | None = None,
        ssl_context_cache: dict | None = None,
        on_connection_change: Callable[[bool], Awaitable[None]] | None = None,
        connection_grace_period: float = DEFAULT_CONNECTION_GRACE_PERIOD,
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
        self.connected: bool = False
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._reported_connected: bool | None = None
        self._disconnect_grace_task: asyncio.Task | None = None

    async def _handle_message(self, topic: str, payload_raw: str) -> None:
        try:
            payload = json.loads(payload_raw)
        except Exception:
            payload = payload_raw

        await self.message_handler(topic, payload)

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

    async def run(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            ssl_ctx = await loop.run_in_executor(None, get_ssl_context, self.config, self.ssl_context_cache)
        except Exception as e:
            _LOGGER.error("Harbor: Failed to create SSL context for camera %s: %s", self.config.serial, e)
            # Ensure we clear any partial state
            self._invalidate_ssl_cache()
            return

        reconnect_delay = 2

        _LOGGER.info("Harbor: MQTT client starting for camera %s", self.config.serial)

        try:
            while not self._stop_event.is_set():
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
        if self._reported_connected:
            await self._notify_connection_change(False)

    def __del__(self) -> None:
        if self._stop_event and not self._stop_event.is_set():
            self._stop_event.set()
