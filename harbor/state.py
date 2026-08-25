"""State models for Harbor devices."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

HarborSourceType = Literal["camera", "monitor"]


@dataclass(slots=True)
class HarborViewer:
    """A viewer connected to a Harbor device stream."""

    viewer_id: str
    identity: str | None = None
    client: str | None = None
    is_local: bool | None = None
    role: str | None = None


@dataclass(slots=True)
class HarborEventState:
    """Record of a Harbor camera detection event.

    Detections are edge triggers: the camera publishes ``motion-detected`` and
    ``sound-anomaly-detected`` when something fires and never publishes a
    counterpart to clear them. There is deliberately no ``is_on`` here -- an
    on/off reading would have to be synthesized from a timer the device knows
    nothing about, and the payload carries nothing that could size one.
    ``last_seen`` is the authoritative signal; consumers that
    need a momentary entity (Home Assistant's ``event`` platform, a device
    trigger) should drive it from a change in ``last_seen``.
    """

    key: str
    topic: str
    friendly_name: str
    last_seen: datetime | None = None
    last_payload: Any = None


@dataclass(slots=True)
class HarborDeviceState:
    """State for a Harbor camera or monitor device."""

    serial: str
    source_type: HarborSourceType
    display_name: str | None = None
    os_version: str | None = None
    app_version: str | None = None
    last_seen: datetime | None = None
    values: dict[str, Any] = field(default_factory=dict)
    viewers: dict[str, HarborViewer] = field(default_factory=dict)
    events: dict[str, HarborEventState] = field(default_factory=dict)
