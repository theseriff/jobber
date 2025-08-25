import asyncio
import functools
from collections.abc import Callable
from typing import ParamSpec, TypeVar

from .base import TaskPlan

_T = TypeVar("_T")
_P = ParamSpec("_P")


class TaskPlanSync(TaskPlan[_T]):
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        func: Callable[_P, _T],
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> None:
        super().__init__(loop, func, *args, **kwargs)

    def _begin(self) -> None:
        callback_injected = functools.partial(
            self.func,
            *self.args,
            **self.kwargs,
        )
        try:
            self._result: _T = callback_injected()
        finally:
            self._event.set()
