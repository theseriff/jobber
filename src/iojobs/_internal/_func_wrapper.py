from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Final, Generic, ParamSpec, TypeVar, overload
from uuid import uuid4

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Awaitable, Callable
    from zoneinfo import ZoneInfo

    from iojobs._internal._types import FuncID
    from iojobs._internal.executors import ExecutorPool
    from iojobs._internal.job_runner import (
        JobRunner,
        JobRunnerAsync,
        JobRunnerSync,
        ScheduledJob,
    )


_P = ParamSpec("_P")
_R = TypeVar("_R")


def create_default_name(func: Callable[_P, _R], /) -> str:
    fname = func.__name__
    fmodule = func.__module__
    if fname == "<lambda>":
        fname = f"lambda_{uuid4().hex}"
    if fmodule == "__main__":
        fmodule = sys.argv[0].removesuffix(".py").replace(os.path.sep, ".")
    return f"{fmodule}:{fname}"


class FuncWrapper(Generic[_P, _R]):
    __slots__: tuple[str, ...] = (
        "_executors",
        "_func_name",
        "_func_registered",
        "_loop",
        "_original_func",
        "_tz",
        "jobs_registered",
    )

    def __init__(
        self,
        *,
        tz: ZoneInfo,
        loop: asyncio.AbstractEventLoop,
        func_name: str,
        original_func: Callable[_P, _R],
        executors_pool: ExecutorPool,
    ) -> None:
        self._tz: Final = tz
        self._loop: asyncio.AbstractEventLoop = loop
        self._executors: Final = executors_pool
        self._func_name: str = func_name
        self._func_registered: dict[FuncID, Callable[_P, _R]] = {}
        self._original_func: Final = original_func
        self.jobs_registered: list[ScheduledJob[_R]] = []

        # This is a hack to make ProcessPoolExecutor work
        # with decorated functions.
        #
        # The problem is that when we decorate a function
        # it becomes a new class. This class has the same
        # name as the original function.
        #
        # When receiver sends original function to another
        # process, it will have the same name as the decorated
        # class. This will cause an error, because ProcessPoolExecutor
        # uses `__name__` and `__qualname__` attributes to
        # import functions from other processes and then it verifies
        # that the function is the same as the original one.
        #
        # This hack renames the original function and injects
        # it back to the module where it was defined.
        # This way ProcessPoolExecutor will be able to import
        # the function by it's name and verify its correctness.
        new_name = f"{original_func.__name__}__iojobs_original"
        original_func.__name__ = new_name
        if hasattr(original_func, "__qualname__"):
            original_qualname = original_func.__qualname__.rsplit(".")
            original_qualname[-1] = new_name
            new_qualname = ".".join(original_qualname)
            original_func.__qualname__ = new_qualname
        setattr(
            sys.modules[original_func.__module__],
            new_name,
            original_func,
        )

    def __call__(
        self,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _R:
        return self._original_func(*args, **kwargs)

    @overload
    async def at(  # type: ignore[overload-overlap]
        self: FuncWrapper[_P, Awaitable[_R]],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> JobRunnerAsync[_R]: ...

    @overload
    async def at(
        self: FuncWrapper[_P, _R],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> JobRunnerSync[_R]: ...

    async def at(
        self,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> JobRunner[_R]:
        _ = args
        _ = kwargs
        raise NotImplementedError

    @overload
    async def cron(  # type: ignore[overload-overlap]
        self: FuncWrapper[_P, Awaitable[_R]],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> JobRunnerAsync[_R]: ...

    @overload
    async def cron(
        self: FuncWrapper[_P, _R],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> JobRunnerSync[_R]: ...
    async def cron(
        self,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> JobRunner[_R]:
        _ = args
        _ = kwargs
        raise NotImplementedError

    @overload
    async def delay(  # type: ignore[overload-overlap]
        self: FuncWrapper[_P, Awaitable[_R]],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> JobRunnerAsync[_R]: ...

    @overload
    async def delay(
        self: FuncWrapper[_P, _R],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> JobRunnerSync[_R]: ...

    async def delay(
        self,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> JobRunner[_R]:
        _ = args
        _ = kwargs
        raise NotImplementedError
