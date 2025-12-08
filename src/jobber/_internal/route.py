# pyright: reportPrivateUsage=false
from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar, overload

from jobber._internal.common.constants import EMPTY
from jobber._internal.exceptions import raise_app_not_started_error
from jobber._internal.runner.scheduler import ScheduleBuilder

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from types import CoroutineType

    from jobber._internal.common.datastructures import State
    from jobber._internal.configuration import (
        JobberConfiguration,
        RouteOptions,
    )
    from jobber._internal.middleware.base import CallNext
    from jobber._internal.runner.runners import RunStrategy


T = TypeVar("T")
ReturnT = TypeVar("ReturnT")
ParamsT = ParamSpec("ParamsT")


class BaseRoute(ABC, Generic[ParamsT, ReturnT]):
    def __init__(
        self,
        options: RouteOptions,
        func: Callable[ParamsT, ReturnT],
    ) -> None:
        self.options: RouteOptions = options
        self.original_func: Callable[ParamsT, ReturnT] = func

    def __call__(
        self,
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ReturnT:
        return self.original_func(*args, **kwargs)

    @overload
    def schedule(
        self: BaseRoute[ParamsT, CoroutineType[object, object, T]],
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ScheduleBuilder[T]: ...

    @overload
    def schedule(
        self: BaseRoute[ParamsT, Coroutine[object, object, T]],
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ScheduleBuilder[T]: ...

    @overload
    def schedule(
        self: BaseRoute[ParamsT, ReturnT],
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ScheduleBuilder[ReturnT]: ...

    @abstractmethod
    def schedule(
        self,
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ScheduleBuilder[Any]:
        raise NotImplementedError


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


class DeferredRoute(BaseRoute[ParamsT, ReturnT]):
    def __init__(
        self,
        options: RouteOptions,
        func: Callable[ParamsT, ReturnT],
    ) -> None:
        super().__init__(options, func)
        self._real_route: JobRoute[ParamsT, ReturnT] | None = None

    def bind(self, route: JobRoute[ParamsT, ReturnT]) -> None:
        self._real_route = route

    def schedule(
        self,
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ScheduleBuilder[Any]:
        if self._real_route is None:
            fname = self.original_func.__name__
            msg = (
                f"Job {fname!r} is not attached to any Jobber app."
                " Did you forget to call app.include_router()?"
            )
            raise RuntimeError(msg)
        return self._real_route.schedule(*args, **kwargs)
