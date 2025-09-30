"""Custom exceptions for the iojobs library.

This module defines specific exceptions that the iojobs scheduling
system can raise. These exceptions provide more detailed information
about errors and guidance on how to handle common scheduling scenarios.
"""

from iojobs._internal.exceptions import (
    ConcurrentExecutionError,
    IOJobsBaseError,
    JobNotCompletedError,
    JobNotInitializedError,
    NegativeDelayError,
)

__all__ = (
    "ConcurrentExecutionError",
    "IOJobsBaseError",
    "JobNotCompletedError",
    "JobNotInitializedError",
    "NegativeDelayError",
)
