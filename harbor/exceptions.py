from __future__ import annotations

from typing import Any

#: Status returned when the firmware has no handler registered for a command.
#: This is permanent for a given firmware build, not a transient failure.
RESOURCE_NOT_FOUND_STATUS = "RESOURCE_NOT_FOUND"


class HarborCommandError(Exception):
    """Raised when a Harbor camera rejects a command.

    ``status`` is the parsed ``status`` field of the camera response (upper
    cased), or ``None`` when the camera reported a bare ``error`` with no
    status. ``errors`` holds the per-field ``errors`` array the firmware
    returns for validation failures, each entry carrying ``error_code``,
    ``key``, ``value`` and the accepted ``schema``.
    """

    def __init__(self, command: str, response: Any) -> None:
        self.command = command
        self.response = response
        self.status = _parse_status(response)
        self.errors = _parse_errors(response)
        super().__init__(f"Harbor camera rejected command {command!r}: {response!r}")


class HarborUnsupportedCommandError(HarborCommandError):
    """Raised when the camera firmware has no handler for a command.

    The camera answered ``RESOURCE_NOT_FOUND``, which means this firmware
    build does not implement the command at all. Retrying cannot succeed, so
    consumers should skip the feature (for example, decline to create an
    entity) rather than surface a failure on every use.
    """


def _parse_status(response: Any) -> str | None:
    """Return the upper-cased status string from a camera response."""
    if not isinstance(response, dict):
        return None
    status = response.get("status")
    if status is None:
        return None
    return str(status).upper()


def _parse_errors(response: Any) -> list[Any]:
    """Return the per-field error details from a camera response."""
    if not isinstance(response, dict):
        return []
    errors = response.get("errors")
    if isinstance(errors, list):
        return errors
    return []
