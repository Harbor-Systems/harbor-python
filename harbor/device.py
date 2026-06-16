from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import UTC
from typing import Any, TypeVar, overload

from pydantic import BaseModel

from .data.mqtt_models import (
    HeartbeatEvent as HarborHeartbeatPayload,
)
from .data.mqtt_models import (
    SettingsEvent as HarborSettingsPayload,
)
from .events import (
    HarborEvent,
    HeartbeatUpdate,
    SettingsUpdate,
    SubscriptionManager,
    parse_message,
)
from .state import HarborDeviceState, HarborSourceType

LOGGER = logging.getLogger(__name__)

UpdateCallbackType = Callable[[HarborDeviceState], Any]
EventT = TypeVar("EventT", bound=HarborEvent)


class HarborDevice(ABC):
    """Abstract base class for Harbor devices."""

    def __init__(self, serial: str, source_type: HarborSourceType) -> None:
        self.serial = serial
        self._source_type = source_type
        self.state = HarborDeviceState(serial=serial, source_type=source_type)
        self._subscriptions = SubscriptionManager()
        self._update_subscriptions: list[UpdateCallbackType] = []

    @property
    def source_type(self) -> HarborSourceType:
        """Return the source type for events from this device."""
        return self._source_type

    @abstractmethod
    def get_topics(self) -> list[str]:
        """Return a list of topics to subscribe to."""
        pass

    async def handle_message(self, topic: str, payload: Any) -> HarborEvent | None:
        """Handle an incoming MQTT message."""
        if not (event := parse_message(topic, payload)):
            return None

        if event.source_type != self.source_type or event.source_sn != self.serial:
            return None

        self._apply_event(event)
        await self._emit_event(event)
        await self._emit_update()
        return event

    @overload
    def subscribe(
        self,
        event_type: type[EventT],
        callback: Callable[[EventT], Any],
    ) -> Callable[[], None]: ...

    @overload
    def subscribe(
        self,
        event_type: type[BaseModel],
        callback: Callable[[HarborEvent], Any],
    ) -> Callable[[], None]: ...

    @overload
    def subscribe(
        self,
        event_type: None,
        callback: Callable[[HarborEvent], Any],
    ) -> Callable[[], None]: ...

    def subscribe(
        self,
        event_type: type[HarborEvent] | type[BaseModel] | None,
        callback: Callable[[Any], Any],
    ) -> Callable[[], None]:
        """Subscribe to a specific Harbor event type or all events."""

        return self._subscriptions.subscribe(event_type, callback)

    async def _emit_event(self, event: HarborEvent) -> None:
        """Emit a parsed Harbor event to subscribers."""

        await self._subscriptions.emit(event)

    def subscribe_updates(
        self,
        callback: UpdateCallbackType,
    ) -> Callable[[], None]:
        """Subscribe to device state updates."""
        self._update_subscriptions.append(callback)

        def unsubscribe() -> None:
            if callback in self._update_subscriptions:
                self._update_subscriptions.remove(callback)

        return unsubscribe

    async def _emit_update(self) -> None:
        """Emit a state update to subscribers."""
        for callback in list(self._update_subscriptions):
            try:
                result = callback(self.state)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                LOGGER.exception(
                    "Error in device update callback %s for %s",
                    getattr(callback, "__qualname__", repr(callback)),
                    self.serial,
                )

    def shutdown(self) -> None:
        """Release device resources."""

    def _apply_event(self, event: HarborEvent) -> None:
        """Apply a parsed Harbor event to device state.

        Subclasses should override this to handle device-specific event types,
        calling ``super()._apply_event(event)`` to keep the shared handling.
        """
        self.state.last_seen = event.timestamp.astimezone(UTC)
        self._update_shared_metadata(event)

        match event:
            case HeartbeatUpdate(payload=payload):
                self._apply_heartbeat(payload)
            case SettingsUpdate(payload=payload):
                self._apply_settings(payload)

    def _update_shared_metadata(self, event: HarborEvent) -> None:
        """Update metadata shared across Harbor event types."""
        if event.os_version is not None:
            self.state.os_version = event.os_version

        if event.app_version is not None:
            self.state.app_version = event.app_version

        if event.display_name is not None:
            self.state.display_name = event.display_name

    def _apply_heartbeat(self, payload: HarborHeartbeatPayload) -> None:
        """Apply a device heartbeat payload."""
        self._set_state_value("temperature", payload.temperature)
        self._set_state_value("sensor_temperature", payload.sensor_temperature)
        self._set_state_value("raw_temperature", payload.raw_temperature)
        self._set_state_value(
            "image_sensor_temperature",
            payload.image_sensor_temperature,
        )
        self._set_state_value("ntc_temperature", payload.ntc_temperature)

    def _apply_settings(self, payload: HarborSettingsPayload) -> None:
        """Apply a settings payload."""
        if payload.state is None:
            return

        self._set_state_value("wifi_strength", payload.state.network_bars)
        self._set_state_value("temperature", payload.state.temperature)

    def _set_state_value(self, key: str, value: Any) -> None:
        """Set a state value when the payload exposed a concrete value."""
        if value is not None:
            self.state.values[key] = value
