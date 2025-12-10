# pyright: reportPrivateUsage=false
from __future__ import annotations

import functools
import os
import sys
import uuid
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast

from jobber._internal.common.constants import EMPTY
from jobber._internal.exceptions import (
    raise_app_already_started_error,
    raise_app_not_started_error,
)
from jobber._internal.routers.abc import BaseRegistrator, BaseRoute
from jobber._internal.runner.runners import create_run_strategy
from jobber._internal.runner.scheduler import ScheduleBuilder

if TYPE_CHECKING:
    from collections.abc import Callable

    from jobber._internal.common.datastructures import State
    from jobber._internal.configuration import (
        JobberConfiguration,
        RouteOptions,
    )
    from jobber._internal.middleware.abc import CallNext
    from jobber._internal.runner.runners import RunStrategy


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


class JobRoute(BaseRoute[ParamsT, ReturnT]):
    def __init__(  # noqa: PLR0913
        self,
        *,
        state: State,
        func: Callable[ParamsT, ReturnT],
        func_name: str,
        strategy: RunStrategy[ParamsT, ReturnT],
        options: RouteOptions,
        jobber_config: JobberConfiguration,
    ) -> None:
        super().__init__(options, func)
        self._strategy_run: RunStrategy[ParamsT, ReturnT] = strategy
        self._middleware_chain: CallNext = EMPTY
        self.jobber_config: JobberConfiguration = jobber_config
        self.func_name: str = func_name
        self.state: State = state

        # --------------------------------------------------------------------
        # HACK: ProcessPoolExecutor / Multiprocessing  # noqa: ERA001, FIX004
        #
        # Problem: `ProcessPoolExecutor` (used for ExecutionMode.PROCESS)
        # serializes the function by its name. When we use `@register`
        # as a decorator, the function's name in the module (`my_func`)
        # now points to the `FuncWrapper` object, not the original function.
        # This breaks `pickle`.
        #
        # Solution: We rename the *original* function (adding a suffix)
        # and "inject" it back into its own module under this new
        # name. This way, `ProcessPoolExecutor` can find and pickle it.
        #
        # We DO NOT apply this hack in two cases (Guard Clauses):
        # 1. If `register` is used as a direct function call (`reg(my_func)`),
        #    because `my_func` in the module still points to the original.
        # 2. If the function has already been renamed (protects from re-entry).
        # --------------------------------------------------------------------

        # Guard 1: Protect against double-renaming
        if func.__name__.endswith("jobber_original"):
            return

        # Guard 2: Check if `register` is used as a decorator (@)
        # or as a direct function call.
        module = sys.modules[func.__module__]
        module_attr = getattr(module, func.__name__, None)
        if module_attr is func:
            return

        # Apply the hack: rename and inject back into the module
        new_name = f"{func.__name__}__jobber_original"
        func.__name__ = new_name
        if hasattr(func, "__qualname__"):  # pragma: no cover
            original_qualname = func.__qualname__.rsplit(".", 1)
            original_qualname[-1] = new_name
            new_qualname = ".".join(original_qualname)
            func.__qualname__ = new_qualname
        setattr(module, new_name, func)

    def schedule(
        self,
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ScheduleBuilder[Any]:
        if not self.jobber_config.app_started:
            raise_app_not_started_error("schedule")

        return ScheduleBuilder(
            state=self.state,
            options=self.options,
            func_name=self.func_name,
            jobber_config=self.jobber_config,
            middleware_chain=self._middleware_chain,
            runnable=self._strategy_run.create_runnable(*args, **kwargs),
        )


class JobRegistrator(BaseRegistrator[JobRoute[..., Any]]):
    def __init__(
        self,
        state: State,
        jobber_config: JobberConfiguration,
    ) -> None:
        super().__init__()
        self.state: State = state
        self.jobber_config: JobberConfiguration = jobber_config

    def register(
        self,
        func: Callable[ParamsT, ReturnT],
        options: RouteOptions,
    ) -> JobRoute[ParamsT, ReturnT]:
        if self.jobber_config.app_started is True:
            raise_app_already_started_error("register")

        fname = options.func_name or create_default_name(func)
        if self._routes.get(fname) is None:
            strategy = create_run_strategy(
                func,
                self.jobber_config,
                mode=options.run_mode,
            )
            route = JobRoute(
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
