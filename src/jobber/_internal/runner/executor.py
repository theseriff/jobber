import asyncio
import functools
from typing import Generic, TypeVar, cast, final

from jobber._internal.common.constants import ExecutionMode
from jobber._internal.context import AppContext

_ReturnType = TypeVar("_ReturnType")


@final
class Executor(Generic[_ReturnType]):
    __slots__: tuple[str, ...] = (
        "app_ctx",
        "execution_mode",
        "func_injected",
    )

    def __init__(
        self,
        *,
        execution_mode: ExecutionMode,
        func_injected: functools.partial[_ReturnType],
        app_ctx: AppContext,
    ) -> None:
        self.app_ctx = app_ctx
        self.func_injected = func_injected
        self.execution_mode = execution_mode

    async def __call__(self) -> _ReturnType:
        handler = self.func_injected
        if asyncio.iscoroutinefunction(handler):
            return cast("_ReturnType", await handler())
        match self.execution_mode:
            case ExecutionMode.THREAD:
                return await self.app_ctx.loop.run_in_executor(
                    self.app_ctx.executors.threadpool,
                    handler,
                )
            case ExecutionMode.PROCESS:
                return await self.app_ctx.loop.run_in_executor(
                    self.app_ctx.executors.processpool,
                    handler,
                )
            case _:
                return handler()
