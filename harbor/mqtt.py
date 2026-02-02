import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any

from aiomqtt import Client, MqttError

from .config import HarborCameraConfig
from .utils import get_camera_host, get_ssl_context

_LOGGER = logging.getLogger(__name__)


def _should_drop_message(topic: str) -> bool:
    return False


class HarborMQTTClient:
    def __init__(
        self,
        camera_config: HarborCameraConfig,
        message_callback: Callable[[str, str, Any], None] | None = None,
        ssl_context_cache: dict | None = None,
    ) -> None:
        self.camera_config = camera_config
        self.message_callback = message_callback
        self.ssl_context_cache = ssl_context_cache or {}
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None

    def _handle_message(self, topic: str, payload_raw: str) -> None:
        try:
            payload = json.loads(payload_raw)
        except Exception:
            payload = payload_raw

        if self.message_callback:
            self.message_callback(self.camera_config.serial, topic, payload)
        else:
            _LOGGER.debug("Harbor: MQTT message on %s: %s", topic, payload)

    async def run(self) -> None:
        ssl_ctx = get_ssl_context(self.camera_config, self.ssl_context_cache)
        reconnect_delay = 2

        topics = [
            f"cameras/{self.camera_config.serial}/events/#",
            "monitors/+/events/#",
        ]

        try:
            while not self._stop_event.is_set():
                try:
                    async with Client(
                        hostname=get_camera_host(self.camera_config),
                        port=8884,
                        tls_context=ssl_ctx,
                    ) as client:
                        _LOGGER.debug("Harbor: MQTT connected for camera %s", self.camera_config.serial)
                        await client.subscribe([(t, 0) for t in topics])
                        async for message in client.messages:
                            _LOGGER.debug("Harbor: MQTT message on %s: %s", message.topic, message.payload)
                            if self._stop_event.is_set():
                                break

                            payload_raw = message.payload.decode("utf-8", errors="replace")
                            topic = str(message.topic)

                            if _should_drop_message(topic):
                                continue

                            self._handle_message(topic, payload_raw)

                        reconnect_delay = 2

                except MqttError as e:
                    _LOGGER.warning("Harbor: MQTT error for %s: %s (reconnecting)", self.camera_config.serial, e)
                except (asyncio.CancelledError, Exception):
                    raise

                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=reconnect_delay)
                except TimeoutError:
                    pass
                reconnect_delay = min(reconnect_delay * 2, 30)

        except asyncio.CancelledError:
            _LOGGER.info("Harbor: MQTT task cancelled for camera %s", self.camera_config.serial)
            raise

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def __del__(self) -> None:
        if self._stop_event and not self._stop_event.is_set():
            self._stop_event.set()
