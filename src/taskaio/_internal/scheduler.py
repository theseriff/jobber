import asyncio
import functools
import textwrap
import warnings
from collections.abc import Callable
from datetime import datetime
from enum import Enum, auto
from typing import Any, Final, Generic, ParamSpec, TypeVar

from taskaio._internal._types import EMPTY
from taskaio._internal.exceptions import (
    TaskNotCompletedError,
    TimerHandlerUninitializedError,
)

T = TypeVar("T")
P = ParamSpec("P")


class ExecutionMode(str, Enum):
    CURRENT = auto()
    THREAD = auto()
    PROCESS = auto()


class TaskPlan(Generic[T]):
    __slots__: tuple[str, ...] = (
        "_event",
        "_loop",
        "_result",
        "_timer_handler",
        "args",
        "delay_seconds",
        "execution_mode",
        "func",
        "is_planned",
        "kwargs",
        "task_id",
    )

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        func: Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        self._event: asyncio.Event = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop = loop
        self._result: T = EMPTY
        self._timer_handler: asyncio.TimerHandle = EMPTY
        self.func: Final = func
        self.args: Final = args
        self.kwargs: Final = kwargs
        self.delay_seconds: float = 0
        self.execution_mode: ExecutionMode = ExecutionMode.CURRENT
        self.is_planned: bool = False
        self.task_id: str = EMPTY

    def set_task_id(self, task_id: str, /) -> "TaskPlan[T]":
        self.task_id = task_id
        return self

    def at(self, at: datetime, /) -> "TaskPlan[T]":
        timestamp_now = datetime.now(tz=at.tzinfo).timestamp()
        timestamp_target = at.timestamp()
        delay_seconds = timestamp_target - timestamp_now
        return self.delay(delay_seconds)

    def delay(self, delay_seconds: float, /) -> "TaskPlan[T]":
        self.delay_seconds = delay_seconds
        if delay_seconds < 0:
            warnings.warn(
                f"Negative delay_seconds ({delay_seconds}) is not supported; "
                "using 0 instead. Please provide non-negative values.",
                UserWarning,
                stacklevel=2,
            )
        else:
            timer_handler = self._loop.call_later(delay_seconds, self._begin)
            self._timer_handler = timer_handler
            self.is_planned = True
            if self.task_id is EMPTY:
                task_id = textwrap.dedent(f"""\
                    func_name={self.func.__name__}, args={self.args}, \
                    kwargs={self.kwargs}, delay_seconds={self.delay_seconds}
                """)
                _ = self.set_task_id(task_id)

        return self

    def is_done(self) -> bool:
        return self._event.is_set()

    @property
    def result(self) -> T:
        if self._result is EMPTY:
            raise TaskNotCompletedError
        return self._result

    @property
    def timer_handler(self) -> asyncio.TimerHandle:
        if self._timer_handler is EMPTY:
            raise TimerHandlerUninitializedError
        return self._timer_handler

    async def wait(self) -> None:
        if not self.is_planned:
            warnings.warn(
                "Cannot wait for unplanned task. "
                "Call at(..) or delay(..) first.",
                category=RuntimeWarning,
                stacklevel=2,
            )
        elif self.is_done():
            warnings.warn(
                "Task is already done - waiting for completion is unnecessary",
                category=RuntimeWarning,
                stacklevel=2,
            )
        else:
            _ = await self._event.wait()

    def _begin(self) -> None:
        callback_injected = functools.partial(
            self.func,
            *self.args,
            **self.kwargs,
        )
        if asyncio.iscoroutinefunction(callback_injected):
            task = asyncio.create_task(callback_injected())
            task.add_done_callback(self._done_async_task)
        else:
            self._done_sync_task(callback_injected)

    def _done_async_task(self, task: asyncio.Task[T]) -> None:
        self._result = task.result()
        self._event.set()

    def _done_sync_task(self, callback: Callable[..., T]) -> None:
        self._result = callback()
        self._event.set()


class TaskScheduler:
    _loop: asyncio.AbstractEventLoop

    def __init__(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self._loop = loop or asyncio.get_running_loop()
        self._tasks: list[TaskPlan[Any]] = []  # pyright: ignore[reportExplicitAny]

    def execute(
        self,
        func: Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> TaskPlan[T]:
        task_delayed = TaskPlan(
            self._loop,
            func,
            *args,
            **kwargs,
        )
        self._tasks.append(task_delayed)
        return task_delayed

    async def wait_for_complete(self) -> None:
        self._tasks.sort(key=lambda t: t.delay_seconds, reverse=True)
        while self._tasks:
            task = self._tasks.pop()
            await task.wait()
