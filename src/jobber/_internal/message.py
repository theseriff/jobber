from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from jobber._internal.configuration import Cron


@dataclass(slots=True, kw_only=True)
class Message:
    route_name: str
    job_id: str
    kwargs: dict[str, Any]
    args: list[Any]
    cron: Cron | None = None
    options: dict[str, Any]
    at_timestamp: float | None = None
