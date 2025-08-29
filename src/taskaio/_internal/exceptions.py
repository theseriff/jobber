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
