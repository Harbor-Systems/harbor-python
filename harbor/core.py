import logging
from typing import Any

from .config import HarborCameraConfig
from .device import HarborDevice
from .mqtt import HarborMQTTClient

_LOGGER = logging.getLogger(__name__)


class Harbor:
    """
    High-level API for managing Harbor devices and MQTT connections.
    """

    def __init__(self) -> None:
        self._devices: dict[str, HarborDevice] = {}
        self._clients: dict[str, HarborMQTTClient] = {}
        self._topics_cache: set[str] = set()

    def add_device(self, device: HarborDevice) -> None:
        """Add a device (camera or monitor) to be managed."""
        self._devices[device.serial] = device
        self._topics_cache.update(device.get_topics())
        _LOGGER.debug("Added device: %s (%s)", device.serial, type(device).__name__)

    def add_camera_connection(self, config: HarborCameraConfig) -> None:
        """
        Add a camera connection configuration.
        This creates an MQTT client for the specified camera.
        """
        if config.serial in self._clients:
            _LOGGER.warning("Camera connection already exists: %s", config.serial)
            return
        topics = list(self._topics_cache)
        client = HarborMQTTClient(
            config=config,
            topics=topics,
            message_handler=self.handle_message,
            client_id=f"harbor-client-{config.serial}",
        )
        self._clients[config.serial] = client
        _LOGGER.info("Added MQTT client for camera: %s", config.serial)

    async def start(self) -> None:
        """Start all MQTT clients."""
        for client in self._clients.values():
            await client.start()

    async def stop(self) -> None:
        """Stop all MQTT clients and release device resources."""
        for client in self._clients.values():
            await client.stop()
        for device in self._devices.values():
            device.shutdown()

    async def handle_message(self, topic: str, payload: Any) -> None:
        """
        Central message handler.
        Dispatches messages to all interested devices.
        """
        for device in self._devices.values():
            await device.handle_message(topic, payload)
