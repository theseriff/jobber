import asyncio
import functools
from collections.abc import Callable
from typing import Final, ParamSpec, TypeVar

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
        self._func_injected: Final = functools.partial(
            self._func,
            *self._args,
            **self._kwargs,
        )

    def _begin(self) -> None:
        try:
            self._result: _T = self._func_injected()
        finally:
            self._event.set()
