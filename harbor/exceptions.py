from __future__ import annotations

from typing import Any


class HarborCommandError(Exception):
    """Raised when a Harbor camera rejects a command."""

    def __init__(self, command: str, response: Any) -> None:
        self.command = command
        self.response = response
        super().__init__(f"Harbor camera rejected command {command!r}: {response!r}")
