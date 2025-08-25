import asyncio
import warnings
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime
from typing import Final, Generic, ParamSpec, TypeVar

from taskaio._internal._types import EMPTY
from taskaio._internal.exceptions import (
    TaskNotCompletedError,
    TimerHandlerUninitializedError,
)

_T = TypeVar("_T")
_P = ParamSpec("_P")


class TaskPlan(Generic[_T], ABC):
    __slots__: tuple[str, ...] = (
        "_event",
        "_loop",
        "_result",
        "_task_id",
        "_timer_handler",
        "args",
        "delay_seconds",
        "func",
        "is_planned",
        "kwargs",
    )

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        func: Callable[_P, _T],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> None:
        self._event: asyncio.Event = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop = loop
        self._result: _T = EMPTY
        self._timer_handler: asyncio.TimerHandle = EMPTY
        self._task_id: str = EMPTY
        self.func: Final = func
        self.args: Final = args
        self.kwargs: Final = kwargs
        self.delay_seconds: float = 0
        self.is_planned: bool = False

    @abstractmethod
    def _begin(self) -> None:
        raise NotImplementedError

    @property
    def task_id(self) -> str:
        return self._task_id or (  # fallback
            f"func_name={self.func.__name__}, args={self.args}, "
            f"kwargs={self.kwargs}, delay_seconds={self.delay_seconds}"
        )

    def at(self, at: datetime, /) -> "TaskPlan[_T]":
        timestamp_now = datetime.now(tz=at.tzinfo).timestamp()
        timestamp_target = at.timestamp()
        delay_seconds = timestamp_target - timestamp_now
        return self.delay(delay_seconds)

    def delay(self, delay_seconds: float, /) -> "TaskPlan[_T]":
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

        return self

    def is_done(self) -> bool:
        return self._event.is_set()

    @property
    def result(self) -> _T:
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
