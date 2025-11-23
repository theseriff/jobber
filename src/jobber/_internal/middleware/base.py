# pyright: reportExplicitAny=false
from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, Protocol, runtime_checkable

from jobber._internal.context import JobContext

CallNext = Callable[[JobContext], Awaitable[Any]]


@runtime_checkable
class BaseMiddleware(Protocol, metaclass=ABCMeta):
    @abstractmethod
    async def __call__(self, call_next: CallNext, context: JobContext) -> Any:  # noqa: ANN401
        pass
