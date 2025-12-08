import functools
import os
import sys
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from typing import Any, ParamSpec, TypeVar, cast, overload

from jobber._internal.common.constants import EMPTY, RunMode
from jobber._internal.common.datastructures import State
from jobber._internal.configuration import JobberConfiguration, RouteOptions
from jobber._internal.exceptions import raise_app_already_started_error
from jobber._internal.route import BaseRoute, JobRoute
from jobber._internal.runner.runners import create_run_strategy

ReturnT = TypeVar("ReturnT")
ParamsT = ParamSpec("ParamsT")

PENDING_CRON_JOBS = "__pending_cron_jobs__"


def create_default_name(func: Callable[ParamsT, ReturnT], /) -> str:
    fname = func.__name__
    fmodule = func.__module__
    if fname == "<lambda>":
        fname = f"lambda_{uuid.uuid4().hex}"
    if fmodule == "__main__":
        fmodule = sys.argv[0].removesuffix(".py").replace(os.path.sep, ".")
    return f"{fmodule}:{fname}"


class BaseRegistrator(ABC):
    @overload
    def __call__(
        self,
        func: Callable[ParamsT, ReturnT],
    ) -> BaseRoute[ParamsT, ReturnT]: ...

    @overload
    def __call__(
        self,
        *,
        retry: int = 0,
        timeout: float = 600,
        max_cron_failures: int = 10,
        run_mode: RunMode = EMPTY,
        func_name: str | None = None,
        cron: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> Callable[
        [Callable[ParamsT, ReturnT]], BaseRoute[ParamsT, ReturnT]
    ]: ...

    @overload
    def __call__(
        self,
        func: Callable[ParamsT, ReturnT],
        *,
        retry: int = 0,
        timeout: float = 600,
        max_cron_failures: int = 10,
        run_mode: RunMode = EMPTY,
        func_name: str | None = None,
        cron: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> BaseRoute[ParamsT, ReturnT]: ...

    def __call__(  # noqa: PLR0913
        self,
        func: Callable[ParamsT, ReturnT] | None = None,
        *,
        retry: int = 0,
        timeout: float = 600,  # default 10 min.
        max_cron_failures: int = 10,
        run_mode: RunMode = EMPTY,
        func_name: str | None = None,
        cron: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> (
        BaseRoute[ParamsT, ReturnT]
        | Callable[[Callable[ParamsT, ReturnT]], BaseRoute[ParamsT, ReturnT]]
    ):
        if max_cron_failures < 1:
            msg = (
                "max_cron_failures must be >= 1."
                " Use 1 for 'stop on first error'."
            )
            raise ValueError(msg)

        route_options = RouteOptions(
            retry=retry,
            timeout=timeout,
            max_cron_failures=max_cron_failures,
            run_mode=run_mode,
            func_name=func_name,
            cron=cron,
            metadata=metadata,
        )

        wrapper = self.register(route_options)
        if callable(func):
            return wrapper(func)
        return wrapper  # pragma: no cover

    @abstractmethod
    def register(
        self,
        options: RouteOptions,
    ) -> Callable[[Callable[ParamsT, ReturnT]], BaseRoute[ParamsT, ReturnT]]:
        raise NotImplementedError


class JobRegistrator(BaseRegistrator):
    def __init__(
        self,
        jobber_config: JobberConfiguration,
        state: State,
    ) -> None:
        self._routes: dict[str, JobRoute[..., Any]] = {}
        self.jobber_config: JobberConfiguration = jobber_config
        self.state: State = state

    def register(
        self,
        options: RouteOptions,
    ) -> Callable[[Callable[ParamsT, ReturnT]], JobRoute[ParamsT, ReturnT]]:
        if self.jobber_config.app_started is True:
            raise_app_already_started_error("register")

        def wrapper(
            func: Callable[ParamsT, ReturnT],
        ) -> JobRoute[ParamsT, ReturnT]:
            fname = options.func_name or create_default_name(func)
            if self._routes.get(fname) is None:
                strategy = create_run_strategy(
                    func,
                    self.jobber_config,
                    mode=options.run_mode,
                )
                route = JobRoute[ParamsT, ReturnT](
                    func=func,
                    state=self.state,
                    options=options,
                    func_name=fname,
                    strategy=strategy,
                    jobber_config=self.jobber_config,
                )
                _ = functools.update_wrapper(route, func)
                self._routes[fname] = route
                if options.cron:
                    p = (route, options.cron)
                    self.state.setdefault(PENDING_CRON_JOBS, []).append(p)

            return cast("JobRoute[ParamsT, ReturnT]", self._routes[fname])

        return wrapper
