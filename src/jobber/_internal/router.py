# pyright: reportPrivateUsage=false
from __future__ import annotations

from collections import deque
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Final, ParamSpec, TypeVar

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from jobber._internal.common.types import Lifespan
    from jobber._internal.middleware.base import BaseMiddleware


T = TypeVar("T")
ReturnT = TypeVar("ReturnT")
ParamsT = ParamSpec("ParamsT")
JobRouterT = TypeVar("JobRouterT", bound="Router")


@asynccontextmanager
async def _lifespan_stub(_: Router) -> AsyncIterator[None]:
    yield None


class Router:
    def __init__(
        self,
        *,
        prefix: str | None = None,
        lifespan: Lifespan[JobRouterT] = _lifespan_stub,
        middleware: Sequence[BaseMiddleware] | None = None,
    ) -> None:
        self._root: Router | None = None
        self._parent: Router | None = None
        self._sub_routers: list[Router] = []
        self.prefix: str = f"{prefix}:" if prefix else ""
        self.lifespan: Final = lifespan
        self.middleware: deque[BaseMiddleware] = deque(middleware or [])

    @property
    def root(self) -> Router:
        if self._root is None:
            if self.parent is None:
                self._root = self
            else:
                current_router = self
                while current_router.parent is not None:
                    current_router = current_router.parent
                self._root = current_router

        return self._root

    @property
    def parent(self) -> Router | None:
        return self._parent

    @parent.setter
    def parent(self, router: Router) -> None:
        """Set the parent router for this router (internal use only).

        Do not use this method in own code.
        All routers should be included via the `include_router` method.
        Self- and circular-referencing are not allowed here.
        """
        if self._parent:
            msg = f"Router is already attached to {self._parent!r}"
            raise RuntimeError(msg)
        if self is router:
            msg = "Self-referencing routers is not allowed"
            raise RuntimeError(msg)

        parent: Router | None = router

        while parent is not None:
            if parent.parent is None:
                self._root = parent
            if parent is self:
                msg = "Circular referencing of Router is not allowed"
                raise RuntimeError(msg)
            parent = parent.parent

        self._parent = router
        router._sub_routers.append(self)

    def include_router(self, router: Router) -> None:
        if router.parent is self:
            return
        router.parent = self
