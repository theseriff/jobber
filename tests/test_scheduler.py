# pyright: reportPrivateUsage=false
import asyncio
from datetime import datetime
from unittest.mock import call, patch

import pytest

from jobber import ExecutionMode, Jobber
from jobber._internal.common.cron_parser import CronParser


def f1(num: int) -> int:
    return num + 1


async def f2(num: int) -> int:
    return num + 1


@pytest.mark.parametrize(
    "exec_mode",
    [
        pytest.param(ExecutionMode.MAIN, id="main"),
        pytest.param(ExecutionMode.THREAD, id="thread"),
        pytest.param(ExecutionMode.PROCESS, id="process"),
    ],
)
@pytest.mark.parametrize(
    ("method", "num", "expected"),
    [
        pytest.param("at", 1, 2),
        pytest.param("cron", 3, 4),
        pytest.param("delay", 2, 3),
    ],
)
async def test_jobber(
    now: datetime,
    *,
    method: str,
    num: int,
    expected: int,
    exec_mode: ExecutionMode,
) -> None:
    jobber = Jobber()
    f1_reg = jobber.register(f1, job_name="f1_reg", exec_mode=exec_mode)
    f2_reg = jobber.register(f2, job_name="f2_reg", exec_mode=exec_mode)
    async with jobber:
        if method == "at":
            job_sync = await f1_reg.schedule(num).at(now, now=now)
            job_async = await f2_reg.schedule(num).at(now, now=now)
        elif method == "delay":
            job_sync = await f1_reg.schedule(num).delay(0, now=now)
            job_async = await f2_reg.schedule(num).delay(0, now=now)
        elif method == "cron":
            exp = "* * * * *"
            with patch.object(CronParser, "next_run", return_value=now) as m:
                job_sync = await f1_reg.schedule(num).cron(exp, now=now)
                job_async = await f2_reg.schedule(num).cron(exp, now=now)
            m.assert_has_calls(calls=[call(now=now), call(now=now)])
        else:
            raise NotImplementedError

        _ = await asyncio.gather(job_sync.wait(), job_async.wait())
        if method == "cron":
            await job_sync.cancel()
            await job_async.cancel()

    assert job_sync.result() == expected
    assert job_async.result() == expected
    assert jobber._app_ctx.asyncio_tasks == set()
    assert jobber._job_registry == {}
