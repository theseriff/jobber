import inspect
from datetime import datetime, timedelta
from typing import TypeAlias
from unittest.mock import AsyncMock, Mock
from zoneinfo import ZoneInfo

import pytest

from jobber._internal.cron_parser import CronParser

MockCronParser: TypeAlias = CronParser[Mock]


@pytest.fixture(scope="session")
def now() -> datetime:
    return datetime.now(tz=ZoneInfo("UTC"))


def now_(now: datetime) -> datetime:
    return now + timedelta(microseconds=300)


@pytest.fixture(scope="session")
def cron_parser() -> MockCronParser:
    cron = Mock(spec=MockCronParser)
    cron.create.return_value = cron
    cron.next_run.side_effect = now_
    cron.get_expression.return_value = "* * * * * *"
    return cron


@pytest.fixture
def amock() -> AsyncMock:
    mock = AsyncMock(return_value="test")
    mock.__signature__ = inspect.Signature()
    return mock
