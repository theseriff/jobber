class TaskNotCompletedError(Exception):
    """Raised when trying to access result of incomplete task."""

    def __init__(
        self,
        message: str = (
            "Task result is not ready yet, "
            "please use .wait() and then you can use .result"
        ),
    ) -> None:
        super().__init__(message)


class TimerHandlerUninitializedError(Exception):
    """Raised when attempting to use an uninitialized timer handler.

    This occurs when accessing the timer handler before the task has been
    scheduled. The timer handler is lazily initialized during task scheduling.
    """

    def __init__(
        self,
        message: str = (
            "Timer handler is not initialized - "
            "schedule the task first with at(..) or delay(..)"
        ),
    ) -> None:
        super().__init__(message)


class InvalidTaskTypeError(RuntimeError):
    """Raised when trying to run a non-coroutine function in an async context.

    This indicates a problem with the library implementation,
    where non-coroutines were not properly identified during
    the scheduling process.
    """

    def __init__(
        self,
        *,
        func_type: str,
        func_name: str,
        message: str | None = None,
    ) -> None:
        if message is None:
            message = (
                f"Expected coroutine function, got {func_type}. "
                f"Function {func_name!r} must be async or return coroutine. "
                "This indicates a library implementation error - "
                "non-coroutine functions should be filtered "
                "at scheduling time."
            )
        super().__init__(message)
