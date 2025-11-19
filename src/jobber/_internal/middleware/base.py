# pyright: reportExplicitAny=false
from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Awaitable, Callable
from typing import (
    Any,
    Protocol,
    TypeVar,
    runtime_checkable,
)

from jobber._internal.context import JobContext

_ReturnType = TypeVar("_ReturnType")
CallNext = Callable[[JobContext], Awaitable[_ReturnType]]


@runtime_checkable
class BaseMiddleware(Protocol, metaclass=ABCMeta):
    @abstractmethod
    async def __call__(
        self,
        call_next: CallNext[Any],
        context: JobContext,
    ) -> Any: ...  # noqa: ANN401
