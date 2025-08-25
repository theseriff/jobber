import asyncio
import functools
from typing import TypeVar

from taskaio._internal.exceptions import InvalidTaskTypeError
from .base import TaskPlan

_T = TypeVar("_T")


class TaskPlanAsync(TaskPlan[_T]):
    def _begin(self) -> None:
        callback_injected = functools.partial(
            self.func,
            *self.args,
            **self.kwargs,
        )
        # this is a check for mypy.
        if not asyncio.iscoroutinefunction(callback_injected):
            raise InvalidTaskTypeError(
                func_type=type(self.func).__name__,
                func_name=self.func.__name__,
            )

        task = asyncio.create_task(callback_injected())
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[_T]) -> None:
        self._result: _T = task.result()
        self._event.set()
