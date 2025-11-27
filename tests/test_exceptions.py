from datetime import datetime
from typing import TypedDict
from unittest.mock import Mock

import pytest

from jobber import Jobber
from jobber._internal.exceptions import (
    ApplicationStateError,
    JobNotCompletedError,
    NegativeDelayError,
)


class CommonKwargs(TypedDict):
    now: datetime
    to_thread: bool
    to_process: bool


async def test_jobber_runtime_error() -> None:
    jobber = Jobber()

    @jobber.register
    def f() -> None: ...

    match = (
        "Cannot 'schedule' - application must be 'started', "
        "but is currently 'not started'."
    )
    with pytest.raises(ApplicationStateError, match=match):
        _ = await f.schedule().delay(0)

    async with jobber:
        template = (
            "Cannot {!r} - application must be 'not started', "
            "but is currently 'started'."
        )

        match = template.format("register")
        with pytest.raises(ApplicationStateError, match=match):
            _ = jobber.register(f)

        match = template.format("add_middleware")
        with pytest.raises(ApplicationStateError, match=match):
            jobber.add_middleware(Mock())

        match = template.format("add_exception_handler")
        with pytest.raises(ApplicationStateError, match=match):
            jobber.add_exception_handler(Exception, Mock())


async def test_job_not_completed() -> None:
    jobber = Jobber()

    @jobber.register(job_name="f1")
    def f1(num: int) -> int:
        return num + 1

    async with jobber:
        job = await f1.schedule(1).delay(0)
        match = "Job result is not ready"
        with pytest.raises(JobNotCompletedError, match=match):
            _ = job.result()

        expected_val = 2
        await job.wait()
        assert job.result() == expected_val

        with pytest.raises(NegativeDelayError, match="Negative delay_seconds"):
            _ = await f1.schedule(2).delay(-1)
