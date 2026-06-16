from __future__ import annotations

from ..device import HarborDevice


class HarborMonitor(HarborDevice):
    """Represents a Harbor monitor device."""

    def __init__(self, serial: str) -> None:
        """Initialize the monitor device."""
        super().__init__(serial, "monitor")

    def get_topics(self) -> list[str]:
        """Return topics that should be subscribed for this device."""

        return [f"monitors/{self.serial}/events/#"]
