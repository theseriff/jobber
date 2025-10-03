from __future__ import annotations

from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, overload
from zoneinfo import ZoneInfo

from iojobs._internal._types import EMPTY, JobDepends
from iojobs._internal.durable.sqlite import SQLiteJobRepository
from iojobs._internal.func_wrapper import FuncWrapper
from iojobs._internal.serializers.ast_literal import AstLiteralSerializer

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Awaitable, Callable

    from iojobs._internal.durable.abc import JobRepository
    from iojobs._internal.job_runner import JobRunner, JobRunnerAsync
    from iojobs._internal.serializers.abc import IOJobsSerializer


_P = ParamSpec("_P")
_R = TypeVar("_R")


class JobScheduler:
    __slots__: tuple[str, ...] = ("_wrapper",)

    def __init__(
        self,
        *,
        tz: ZoneInfo = EMPTY,
        loop: asyncio.AbstractEventLoop = EMPTY,
        serializer: IOJobsSerializer = EMPTY,
        durable: JobRepository = EMPTY,
    ) -> None:
        self._wrapper: FuncWrapper[..., Any] = FuncWrapper(  # pyright: ignore[reportExplicitAny]
            loop=loop,
            tz=tz or ZoneInfo("UTC"),
            durable=durable or SQLiteJobRepository(),
            serializer=serializer or AstLiteralSerializer(),
        )

    @overload
    def register(
        self,
        func: Callable[_P, Awaitable[_R]],
    ) -> Callable[_P, JobRunnerAsync[_R]]: ...

    @overload
    def register(
        self,
        func: Callable[_P, _R],
    ) -> Callable[_P, JobRunner[_R]]: ...

    def register(
        self,
        func: Callable[_P, Awaitable[_R] | _R] | None = None,
        *,
        func_id: str | None = None,
    ) -> (
        Callable[_P, JobRunner[_R]]
        | Callable[[Callable[_P, _R]], Callable[_P, JobRunner[_R]]]
    ):
        wrapper = self._wrapper.register(func_id)
        if callable(func):
            return wrapper(func)
        return wrapper

    def add_depends(self, **kwargs: JobDepends) -> None:
        self._wrapper.depends.update(kwargs)

    async def wait_for_complete(self) -> None:
        jobs_scheduled = self._wrapper.jobs_registered
        try:
            while jobs_scheduled:
                job = jobs_scheduled[0]
                await job.wait()
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        self._wrapper.shutdown()
