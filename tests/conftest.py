from datetime import datetime
from zoneinfo import ZoneInfo

import pytest


@pytest.fixture(scope="session")
def now() -> datetime:
    return datetime.now(tz=ZoneInfo("UTC"))
