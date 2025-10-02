from __future__ import annotations

import asyncio
import functools
import os
import sys
from typing import (
    TYPE_CHECKING,
    Final,
    Generic,
    ParamSpec,
    TypeVar,
    cast,
)
from uuid import uuid4

from iojobs._internal._types import FuncID, JobDepends
from iojobs._internal.executors import ExecutorPool
from iojobs._internal.job_executor import (
    JobExecutor,
    JobExecutorAsync,
    JobExecutorSync,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine
    from zoneinfo import ZoneInfo

    from iojobs._internal.durable.abc import JobRepository
    from iojobs._internal.job_executor import ScheduledJob
    from iojobs._internal.serializers.abc import IOJobsSerializer

_P = ParamSpec("_P")
_R = TypeVar("_R")


class FuncWrapper(Generic[_P, _R]):
    __slots__: tuple[str, ...] = (
        "_durable",
        "_executors",
        "_func_registered",
        "_loop",
        "_serializer",
        "_tz",
        "depends",
        "jobs_registered",
    )

    def __init__(
        self,
        *,
        tz: ZoneInfo,
        loop: asyncio.AbstractEventLoop,
        serializer: IOJobsSerializer,
        durable: JobRepository,
    ) -> None:
        self._executors: Final = ExecutorPool()
        self._loop: Final = loop
        self._func_registered: dict[
            FuncID,
            Callable[_P, Coroutine[object, object, _R] | _R],
        ] = {}
        self._tz: Final = tz
        self._durable: JobRepository = durable
        self._serializer: IOJobsSerializer = serializer
        self.depends: JobDepends = {}
        self.jobs_registered: list[ScheduledJob[_R]] = []

    def register(
        self,
        func_id: str | None,
    ) -> Callable[[Callable[_P, _R]], Callable[_P, JobExecutor[_R]]]:
        def wrapper(
            func: Callable[_P, Coroutine[object, object, _R] | _R],
        ) -> Callable[_P, JobExecutor[_R]]:
            _patch_fname(func)
            fn_id = FuncID(func_id or _create_func_id(func))
            self._func_registered[fn_id] = func

            @functools.wraps(func)
            def inner(*args: _P.args, **kwargs: _P.kwargs) -> JobExecutor[_R]:
                job: JobExecutor[_R]
                func_injected = functools.partial(func, *args, **kwargs)
                if asyncio.iscoroutinefunction(func_injected):
                    job = JobExecutorAsync(
                        loop=self._loop,
                        func_id=fn_id,
                        func_injected=func_injected,
                        jobs_registered=self.jobs_registered,
                        tz=self._tz,
                        depends=self.depends,
                    )
                else:
                    job = JobExecutorSync(
                        loop=self._loop,
                        func_id=fn_id,
                        func_injected=cast("Callable[_P, _R]", func_injected),
                        jobs_registered=self.jobs_registered,
                        tz=self._tz,
                        executors=self._executors,
                        depends=self.depends,
                    )
                return job

            return inner

        return wrapper

    def shutdown(self) -> None:
        self._executors.shutdown()


def _create_func_id(func: Callable[_P, _R]) -> str:
    fname = func.__name__
    fmodule = func.__module__
    if fname == "<lambda>":
        fname = f"lambda_{uuid4().hex}"
    if fmodule == "__main__":
        fmodule = sys.argv[0].removesuffix(".py").replace(os.path.sep, ".")
    return f"{fmodule}:{fname}"


def _patch_fname(original_func: Callable[_P, _R]) -> None:
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
