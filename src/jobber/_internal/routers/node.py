# pyright: reportPrivateUsage=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

from jobber._internal.routers.abc import BaseRoute

if TYPE_CHECKING:
    from collections.abc import Callable

    from jobber._internal.configuration import RouteOptions
    from jobber._internal.routers.root import JobRoute
    from jobber._internal.runner.scheduler import ScheduleBuilder


ReturnT = TypeVar("ReturnT")
ParamsT = ParamSpec("ParamsT")


class DeferredRoute(BaseRoute[ParamsT, ReturnT]):
    def __init__(
        self,
        options: RouteOptions,
        func: Callable[ParamsT, ReturnT],
    ) -> None:
        super().__init__(options, func)
        self._real_route: JobRoute[ParamsT, ReturnT] | None = None

    def bind(self, route: JobRoute[ParamsT, ReturnT]) -> None:
        self._real_route = route

    def schedule(
        self,
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ScheduleBuilder[Any]:
        if self._real_route is None:
            fname = self.original_func.__name__
            msg = (
                f"Job {fname!r} is not attached to any Jobber app."
                " Did you forget to call app.include_router()?"
            )
            raise RuntimeError(msg)
        return self._real_route.schedule(*args, **kwargs)
