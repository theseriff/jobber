import asyncio
import functools
from collections.abc import Callable, Coroutine
from typing import Final, ParamSpec, TypeVar

from .base import TaskPlan

_T = TypeVar("_T")
_P = ParamSpec("_P")


class TaskPlanAsync(TaskPlan[_T]):
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        func: Callable[_P, Coroutine[None, None, _T]],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> None:
        super().__init__(loop, func, *args, **kwargs)
        self._func_injected: Final = functools.partial(
            func,
            *args,
            **kwargs,
        )

    def _begin(self) -> None:
        task = asyncio.create_task(self._func_injected())
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[_T]) -> None:
        try:
            self._result: _T = task.result()
        finally:
            self._event.set()
