import logging
from typing import Any

from .config import HarborCameraConfig
from .data.mqtt_models import SettingsEvent
from .device import HarborDevice
from .mqtt import DEFAULT_INITIAL_COMMANDS, HarborMQTTClient

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
        topics = list({*self._topics_cache, f"cameras/{config.serial}/responses/#"})
        client = HarborMQTTClient(
            config=config,
            topics=topics,
            message_handler=self.handle_message,
            client_id=f"harbor-client-{config.serial}",
            initial_commands=DEFAULT_INITIAL_COMMANDS,
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

    async def publish_camera_command(
        self,
        serial: str,
        command: str,
        payload: Any,
    ) -> None:
        """Publish a command to a camera."""
        await self._get_client(serial).publish_command(command, payload)

    async def request_camera_command(
        self,
        serial: str,
        command: str,
        payload: dict[str, Any],
        *,
        timeout: float = 10.0,
    ) -> Any:
        """Publish a command to a camera and wait for the matching response."""
        return await self._get_client(serial).request_command(command, payload, timeout=timeout)

    async def get_camera_settings(
        self,
        serial: str,
        *,
        timeout: float = 10.0,
    ) -> SettingsEvent:
        """Request the camera settings payload."""
        return await self._get_client(serial).get_settings(timeout=timeout)

    async def set_camera_on(
        self,
        serial: str,
        camera_on: bool,
        *,
        viewer_id: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        """Turn a camera stream on or off and refresh its settings."""
        await self._get_client(serial).set_camera_on(
            camera_on,
            viewer_id=viewer_id,
            timeout=timeout,
        )

    async def set_night_mode(
        self,
        serial: str,
        night_mode: bool,
        *,
        timeout: float = 10.0,
    ) -> None:
        """Turn camera night mode on or off and refresh its settings."""
        await self._get_client(serial).set_night_mode(
            night_mode,
            timeout=timeout,
        )

    async def handle_message(self, topic: str, payload: Any) -> None:
        """
        Central message handler.
        Dispatches messages to all interested devices.
        """
        for device in self._devices.values():
            await device.handle_message(topic, payload)

    def _get_client(self, serial: str) -> HarborMQTTClient:
        try:
            return self._clients[serial]
        except KeyError as exc:
            raise KeyError(f"No camera connection exists for serial {serial!r}") from exc
