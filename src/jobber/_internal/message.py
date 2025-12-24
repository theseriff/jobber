from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypedDict

from jobber._internal.configuration import Cron


class CommonArguments(TypedDict):
    now: datetime
    job_id: str


class CronArguments(CommonArguments):
    cron: Cron


class AtArguments(CommonArguments):
    at: datetime


@dataclass(slots=True, kw_only=True)
class Message:
    route_name: str
    job_id: str
    arguments: dict[str, Any]
    cron: CronArguments | None = None
    at: AtArguments | None = None
