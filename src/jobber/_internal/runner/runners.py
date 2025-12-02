from __future__ import annotations

import functools
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Final, Generic, ParamSpec, TypeVar, final

from jobber._internal.common.constants import ExecutionMode

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from concurrent.futures import Executor

    from jobber._internal.common.types import LoopFactory
    from jobber._internal.configuration import (
        JobberConfiguration,
        RouteConfiguration,
    )

ReturnT = TypeVar("ReturnT")
ParamsT = ParamSpec("ParamsT")


class RunStrategy(ABC, Generic[ParamsT, ReturnT]):
    __slots__: tuple[str, ...] = ("func",)

    def __init__(self, func: Callable[ParamsT, ReturnT]) -> None:
        self.func: Final = func

    @abstractmethod
    async def __call__(
        self,
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ReturnT:
        raise NotImplementedError


class SyncStrategy(RunStrategy[ParamsT, ReturnT]):
    async def __call__(
        self,
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ReturnT:
        return self.func(*args, **kwargs)


class AsyncStrategy(RunStrategy[ParamsT, ReturnT]):
    async def __call__(
        self: AsyncStrategy[ParamsT, Awaitable[ReturnT]],
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ReturnT:
        return await self.func(*args, **kwargs)


class PoolStrategy(RunStrategy[ParamsT, ReturnT]):
    __slots__: tuple[str, ...] = ("executor", "loop_factory")

    def __init__(
        self,
        func: Callable[ParamsT, ReturnT],
        executor: Executor | None,
        loop_factory: LoopFactory,
    ) -> None:
        super().__init__(func)
        self.executor: Executor | None = executor
        self.loop_factory: LoopFactory = loop_factory

    async def __call__(
        self,
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> ReturnT:
        loop = self.loop_factory()
        func_call = functools.partial(self.func, *args, **kwargs)
        return await loop.run_in_executor(self.executor, func_call)


@final
class Runnable(Generic[ReturnT]):
    __slots__: tuple[str, ...] = ("args", "kwargs", "strategy")

    def __init__(
        self,
        strategy: RunStrategy[ParamsT, ReturnT],
        /,
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> None:
        self.strategy = strategy
        self.args = args
        self.kwargs = kwargs

    def __call__(self) -> Awaitable[ReturnT]:
        return self.strategy(*self.args, **self.kwargs)


def _create_strategy(
    func: Callable[ParamsT, ReturnT],
    route_config: RouteConfiguration,
    jobber_config: JobberConfiguration,
) -> RunStrategy[ParamsT, ReturnT]:
    if route_config.is_async:
        return AsyncStrategy(func)

    loop_factory = jobber_config.loop_factory
    match route_config.exec_mode:
        case ExecutionMode.PROCESS:
            processpool = jobber_config.worker_pools.processpool
            return PoolStrategy(func, processpool, loop_factory)
        case ExecutionMode.THREAD:
            threadpool = jobber_config.worker_pools.threadpool
            return PoolStrategy(func, threadpool, loop_factory)
        case _:
            return SyncStrategy(func)


def create_runnable_factory(
    func: Callable[ParamsT, ReturnT],
    route_config: RouteConfiguration,
    jobber_config: JobberConfiguration,
) -> Callable[ParamsT, Runnable[ReturnT]]:
    strategy = _create_strategy(func, route_config, jobber_config)

    def create_runnable(
        *args: ParamsT.args,
        **kwargs: ParamsT.kwargs,
    ) -> Runnable[ReturnT]:
        return Runnable(strategy, *args, **kwargs)

    return create_runnable
