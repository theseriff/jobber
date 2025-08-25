import asyncio
import functools
from collections.abc import Callable
from typing import Final, ParamSpec, TypeVar

from taskaio._internal.exceptions import InvalidTaskTypeError
from .base import TaskPlan

_T = TypeVar("_T")
_P = ParamSpec("_P")


class TaskPlanAsync(TaskPlan[_T]):
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        func: Callable[_P, _T],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> None:
        super().__init__(loop, func, *args, **kwargs)
        self._func_injected: Final = functools.partial(
            self._func,
            *self._args,
            **self._kwargs,
        )

    def _begin(self) -> None:
        if not asyncio.iscoroutinefunction(self._func_injected):
            # this is a check for mypy.
            raise InvalidTaskTypeError(
                func_type=type(self._func).__name__,
                func_name=self._func.__name__,
            )

        task = asyncio.create_task(self._func_injected())
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[_T]) -> None:
        try:
            self._result: _T = task.result()
        finally:
            self._event.set()
