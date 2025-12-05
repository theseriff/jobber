from datetime import datetime, timedelta
from unittest import mock
from zoneinfo import ZoneInfo

import pytest

from jobber import Jobber
from jobber._internal.cron_parser import CronParser
from jobber.crontab import Crontab


def test_cronparser() -> None:
    cron = Crontab("@daily")
    now = datetime.now(tz=ZoneInfo("Europe/Moscow"))
    next_run = cron.next_run(now=now)
    expected_run = (now + timedelta(days=1)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    assert cron.get_expression() == "@daily"
    assert next_run == expected_run


async def test_cron_reschedule(
    now: datetime,
    cron_parser_cls: type[CronParser],
) -> None:
    jobber = Jobber(cron_parser_cls=cron_parser_cls)

    @jobber.register
    def t(name: str) -> str:
        return f"hello, {name}!"

    async with jobber:
        job = await t.schedule("Biba").cron("* * * * *", now=now)

        cur_exec_at = job.exec_at
        await job.wait()
        next_exec_at = job.exec_at

        assert job.result() == "hello, Biba!"
        assert cur_exec_at != next_exec_at


async def test_max_cron_failures(
    amock: mock.AsyncMock,
    cron_parser_cls: type[CronParser],
) -> None:
    amock.side_effect = ValueError

    jobber = Jobber(cron_parser_cls=cron_parser_cls)
    match = "max_cron_failures must be >= 1. Use 1 for 'stop on first error'."
    with pytest.raises(ValueError, match=match):
        _ = jobber.register(amock, max_cron_failures=0)

    max_failures = 1
    f = jobber.register(amock, max_cron_failures=max_failures)
    async with jobber:
        expression = "* * * * * * *"  # every seconds
        job = await f.schedule().cron(expression)
        await job.wait()

    amock.assert_awaited_once()
    assert not job.should_reschedule(max_failures)
