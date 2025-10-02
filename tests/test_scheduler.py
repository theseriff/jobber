import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from iojobs._internal.scheduler import JobScheduler

TZ_UTC = ZoneInfo("UTC")
scheduler = JobScheduler(tz=TZ_UTC)


@scheduler.register
def f1(num: int) -> int:
    return num + 1


@scheduler.register
async def f2(num: int) -> int:
    return num + 1


async def test_scheduler_delay() -> None:
    scheduled_job1 = f1(1).to_process().delay(0)
    scheduled_job2 = f2(1).delay(0)
    _ = await asyncio.gather(scheduled_job1.wait(), scheduled_job2.wait())
    expected_val = 2
    assert scheduled_job1.result == scheduled_job2.result == expected_val


async def test_scheduler_at() -> None:
    now = datetime.now(tz=TZ_UTC) + timedelta(microseconds=300)
    scheduled_job1 = f1(2).at(now)
    scheduled_job2 = f2(2).at(now)
    _ = await asyncio.gather(scheduled_job1.wait(), scheduled_job2.wait())
    expected_val = 3
    assert scheduled_job1.result == scheduled_job2.result == expected_val
