"""Custom exceptions for the taskaio library.

This module defines specific exceptions that the taskaio scheduling
system can raise. These exceptions provide more detailed information
about errors and guidance on how to handle common scheduling scenarios.
"""

from taskaio._internal.exceptions import (
    TaskNotCompletedError,
    TimerHandlerUninitializedError,
)

__all__ = ("TaskNotCompletedError", "TimerHandlerUninitializedError")
