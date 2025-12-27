from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeAlias

from jobber._internal.common.datastructures import RequestState, State
from jobber._internal.configuration import (
    JobberConfiguration,
    RouteOptions,
)
from jobber._internal.runners import Runnable
from jobber._internal.scheduler.job import Job

JobberApp: TypeAlias = Callable[["JobContext"], Any]


@dataclass(slots=True, kw_only=True)
class JobContext:
    job: Job[Any]
    state: State
    runnable: Runnable[Any]
    request_state: RequestState
    route_options: RouteOptions
    jobber_config: JobberConfiguration
