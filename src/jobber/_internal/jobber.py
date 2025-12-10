# ruff: noqa: SLF001
# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

from jobber._internal.common.datastructures import State
from jobber._internal.configuration import JobberConfiguration, WorkerPools
from jobber._internal.exceptions import raise_app_already_started_error
from jobber._internal.injection import inject_context
from jobber._internal.middleware.abc import build_middleware
from jobber._internal.middleware.exceptions import (
    ExceptionHandler,
    ExceptionHandlers,
    ExceptionMiddleware,
)
from jobber._internal.middleware.retry import RetryMiddleware
from jobber._internal.middleware.timeout import TimeoutMiddleware
from jobber._internal.routers.root import JobRegistrator

if TYPE_CHECKING:
    from collections import deque
    from collections.abc import AsyncIterator, Callable
    from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
    from types import TracebackType
    from zoneinfo import ZoneInfo

    from jobber._internal.common.types import Lifespan
    from jobber._internal.context import JobContext
    from jobber._internal.cron_parser import CronParser
    from jobber._internal.middleware.abc import BaseMiddleware
    from jobber._internal.runner.runners import Runnable
    from jobber._internal.serializers.abc import JobsSerializer
    from jobber._internal.storage.abc import JobRepository


ReturnT = TypeVar("ReturnT")
ParamsT = ParamSpec("ParamsT")
AppT = TypeVar("AppT", bound="Jobber")


class Jobber:
    def __init__(  # noqa: PLR0913
        self,
        *,
        tz: ZoneInfo,
        durable: JobRepository,
        lifespan: Lifespan[AppT],
        serializer: JobsSerializer,
        middleware: deque[BaseMiddleware],
        exception_handlers: ExceptionHandlers,
        loop_factory: Callable[[], asyncio.AbstractEventLoop],
        threadpool_executor: ThreadPoolExecutor | None,
        processpool_executor: ProcessPoolExecutor | None,
        cron_parser_cls: type[CronParser],
    ) -> None:
        self._lifespan: AsyncIterator[None] = self._run_lifespan(lifespan)
        self._middleware: deque[BaseMiddleware] = middleware
        self._exc_handlers: ExceptionHandlers = exception_handlers
        self.state: State = State()
        self.jobber_config: JobberConfiguration = JobberConfiguration(
            loop_factory=loop_factory,
            tz=tz,
            durable=durable,
            worker_pools=WorkerPools(
                _processpool=processpool_executor,
                threadpool=threadpool_executor,
            ),
            serializer=serializer,
            cron_parser_cls=cron_parser_cls,
            _tasks_registry=set(),
            _jobs_registry={},
        )
        self.task: JobRegistrator = JobRegistrator(
            self.state,
            self.jobber_config,
        )

    def add_exception_handler(
        self,
        cls_exc: type[Exception],
        handler: ExceptionHandler,
    ) -> None:
        if self.jobber_config.app_started is True:
            raise_app_already_started_error("add_exception_handler")
        self._exc_handlers[cls_exc] = handler

    def add_middleware(self, middleware: BaseMiddleware) -> None:
        if self.jobber_config.app_started is True:
            raise_app_already_started_error("add_middleware")
        self._middleware.appendleft(middleware)

    async def _run_lifespan(
        self,
        user_lifespan: Lifespan[Any],
    ) -> AsyncIterator[None]:
        async with user_lifespan(self) as maybe_state:
            if maybe_state is not None:
                self.state.update(maybe_state)
            yield None

    async def _entry(self, context: JobContext) -> Any:  # noqa: ANN401
        runnable: Runnable[Any] = context.runnable
        inject_context(runnable, context)
        return await runnable()

    def _build_middleware_chain(self) -> None:
        system_middlewares = (
            TimeoutMiddleware(),
            RetryMiddleware(),
            ExceptionMiddleware(self._exc_handlers, self.jobber_config),
        )
        self._middleware.extend(system_middlewares)
        middleware_chain = build_middleware(self._middleware, self._entry)
        for route in self.task._routes.values():
            route._middleware_chain = middleware_chain

    async def startup(self) -> None:
        await anext(self._lifespan)
        self._build_middleware_chain()
        self.jobber_config.app_started = True

        if crons := self.state.pop("__pending_cron_jobs__", []):
            pending = (route.schedule().cron(cron) for route, cron in crons)
            _ = await asyncio.gather(*pending)

    async def shutdown(self) -> None:
        self.jobber_config.app_started = False
        if tasks := self.jobber_config._tasks_registry:
            for task in tasks:
                _ = task.cancel()
            _ = await asyncio.gather(*tasks, return_exceptions=True)
            self.jobber_config._tasks_registry.clear()

        self.jobber_config.close()
        await anext(self._lifespan, None)

    async def __aenter__(self) -> Jobber:
        await self.startup()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None:
        await self.shutdown()
