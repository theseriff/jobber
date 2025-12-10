# pyright: reportPrivateUsage=false
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generic, ParamSpec, TypeVar, overload

from jobber._internal.common.constants import EMPTY, RunMode
from jobber._internal.configuration import RouteOptions

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Mapping
    from types import CoroutineType

    from jobber._internal.runner.scheduler import ScheduleBuilder


T = TypeVar("T")
ReturnT = TypeVar("ReturnT")
ParamsT = ParamSpec("ParamsT")
BoundRouteT = TypeVar("BoundRouteT", bound="BaseRoute[..., Any]")


class BaseRoute(ABC, Generic[ParamsT, ReturnT]):
    def __init__(
        self,
        options: RouteOptions,
        func: Callable[ParamsT, ReturnT],
    ) -> None:
        self.options: RouteOptions = options
        self.original_func: Callable[ParamsT, ReturnT] = func

    def __call__(
        self,
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ReturnT:
        return self.original_func(*args, **kwargs)

    @overload
    def schedule(
        self: BaseRoute[ParamsT, CoroutineType[object, object, T]],
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ScheduleBuilder[T]: ...

    @overload
    def schedule(
        self: BaseRoute[ParamsT, Coroutine[object, object, T]],
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ScheduleBuilder[T]: ...

    @overload
    def schedule(
        self: BaseRoute[ParamsT, ReturnT],
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ScheduleBuilder[ReturnT]: ...

    @abstractmethod
    def schedule(
        self,
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ScheduleBuilder[Any]:
        raise NotImplementedError


class BaseRegistrator(ABC, Generic[BoundRouteT]):
    def __init__(self) -> None:
        self._routes: dict[str, BoundRouteT] = {}

    @overload
    def __call__(
        self,
        func: Callable[ParamsT, ReturnT],
    ) -> BaseRoute[ParamsT, ReturnT]: ...

    @overload
    def __call__(
        self,
        *,
        retry: int = 0,
        timeout: float = 600,
        max_cron_failures: int = 10,
        run_mode: RunMode = EMPTY,
        func_name: str | None = None,
        cron: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> Callable[
        [Callable[ParamsT, ReturnT]], BaseRoute[ParamsT, ReturnT]
    ]: ...

    @overload
    def __call__(
        self,
        func: Callable[ParamsT, ReturnT],
        *,
        retry: int = 0,
        timeout: float = 600,
        max_cron_failures: int = 10,
        run_mode: RunMode = EMPTY,
        func_name: str | None = None,
        cron: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> BaseRoute[ParamsT, ReturnT]: ...

    def __call__(  # noqa: PLR0913
        self,
        func: Callable[ParamsT, ReturnT] | None = None,
        *,
        retry: int = 0,
        timeout: float = 600,  # default 10 min.
        max_cron_failures: int = 10,
        run_mode: RunMode = EMPTY,
        func_name: str | None = None,
        cron: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> (
        BaseRoute[ParamsT, ReturnT]
        | Callable[[Callable[ParamsT, ReturnT]], BaseRoute[ParamsT, ReturnT]]
    ):
        if max_cron_failures < 1:
            msg = (
                "max_cron_failures must be >= 1."
                " Use 1 for 'stop on first error'."
            )
            raise ValueError(msg)
        route_options = RouteOptions(
            retry=retry,
            timeout=timeout,
            max_cron_failures=max_cron_failures,
            run_mode=run_mode,
            func_name=func_name,
            cron=cron,
            metadata=metadata,
        )

        wrapper = self._register(route_options)
        if callable(func):
            return wrapper(func)
        return wrapper  # pragma: no cover

    def _register(
        self,
        options: RouteOptions,
    ) -> Callable[[Callable[ParamsT, ReturnT]], BaseRoute[ParamsT, ReturnT]]:
        def wrapper(
            func: Callable[ParamsT, ReturnT],
        ) -> BaseRoute[ParamsT, ReturnT]:
            return self.register(func, options)

        return wrapper

    @abstractmethod
    def register(
        self,
        func: Callable[ParamsT, ReturnT],
        options: RouteOptions,
    ) -> BaseRoute[ParamsT, ReturnT]:
        raise NotImplementedError
