from typing import NoReturn


class BaseJobberError(Exception):
    pass


class JobNotCompletedError(BaseJobberError):
    """Raised when trying to access result of incomplete job."""

    def __init__(
        self,
        message: str = (
            "Job result is not ready yet, "
            "please use .wait() and then you can use .result"
        ),
    ) -> None:
        super().__init__(message)


class JobFailedError(BaseJobberError):
    def __init__(self, job_id: str, reason: str) -> None:
        self.job_id: str = job_id
        self.reason: str = reason
        message = f"job_id: {job_id}, failed_reason: {reason}"
        super().__init__(message)


class NegativeDelayError(BaseJobberError):
    """Exception raised when negative delay_seconds is provided."""

    def __init__(
        self,
        delay_seconds: float,
        message: str = (
            "Negative delay_seconds ({delay_seconds}) is not supported. "
            "Please provide non-negative values."
        ),
    ) -> None:
        super().__init__(message.format(delay_seconds=delay_seconds))
        self.delay_seconds: float = delay_seconds


class HandlerSkippedError(BaseJobberError):
    """Raised when middleware chain completes without calling the job callback.

    This occurs when one of middleware in the chain decides to
    short-circuit the execution and returns early without calling `call_next`,
    preventing the actual job handler from being executed.
    """

    def __init__(
        self,
        message: str = (
            "Job callback was not executed. A middleware in the chain "
            "short-circuited without calling call_next."
        ),
    ) -> None:
        super().__init__(message)


class JobSkippedError(BaseJobberError):
    """Raised when middleware chain completes without calling the job callback.

    This occurs when a middleware in the chain decides to short-circuit
    the execution and returns a response early without calling `call_next`,
    preventing the actual job handler from being executed.
    """

    def __init__(
        self,
        message: str = (
            "Job was not executed. A middleware short-circuited "
            "the request without calling call_next."
        ),
    ) -> None:
        super().__init__(message)


class ApplicationStateError(BaseJobberError):
    """Raised when app is in wrong state for the requested operation."""

    def __init__(
        self,
        *,
        operation: str,
        required_state: str,
        actual_state: str,
    ) -> None:
        message = (
            f"Cannot {operation!r} - application must be {required_state!r}, "
            f"but is currently {actual_state!r}."
        )
        super().__init__(message)


def raise_app_not_started_error(operation: str) -> NoReturn:
    raise ApplicationStateError(
        operation=operation,
        required_state="started",
        actual_state="not started",
    )


def raise_app_already_started_error(operation: str) -> NoReturn:
    raise ApplicationStateError(
        operation=operation,
        required_state="not started",
        actual_state="started",
    )
