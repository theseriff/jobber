import asyncio
from datetime import datetime
from pathlib import Path

import pytest

from jobber import Cron, Jobber, JobStatus
from jobber._internal.message import Message
from jobber._internal.storage.abc import ScheduledJob
from jobber._internal.storage.sqlite import SQLiteStorage
from tests.conftest import create_cron_factory


async def test_sqlite() -> None:
    db = Path("test.db")
    storage = SQLiteStorage(database=db, table_name="test_table")
    storage.threadpool = None
    storage.getloop = asyncio.get_running_loop

    with pytest.raises(RuntimeError):
        _ = storage.conn
    await storage.shutdown()

    scheduled = ScheduledJob("1", "test_name", b"", JobStatus.SUCCESS)
    await storage.startup()
    try:
        await storage.add_schedule(scheduled)
        schedules = await storage.get_schedules()
        assert schedules[0] == scheduled

        await storage.delete_schedule(scheduled.job_id)
        assert await storage.get_schedules() == []
        assert storage.database.exists()
    finally:
        await storage.shutdown()
        db.unlink()


async def test_sqlite_with_jobber(now: datetime) -> None:
    app = Jobber(
        storage=SQLiteStorage(":memory:"),
        cron_factory=create_cron_factory(),
    )

    @app.task
    async def f1() -> str:
        return "test"

    @app.task(durable=False)
    async def f2() -> str:
        return "test"

    async with app:
        job1 = await f1.schedule().delay(0.05, now=now)
        job2 = await f2.schedule().delay(0, now=now)

        cron = Cron("* * * * *", max_runs=1)
        job1_cron = await f1.schedule().cron(cron, now=now, job_id="test")

        raw_msg1 = app.configs.serializer.dumpb(
            app.configs.dumper.dump(
                Message(
                    job_id=job1.id,
                    func_name=f1.name,
                    arguments={},
                    at={"at": job1.exec_at, "job_id": job1.id, "now": now},
                ),
                Message,
            )
        )
        raw_msg2 = app.configs.serializer.dumpb(
            app.configs.dumper.dump(
                Message(
                    job_id=job1_cron.id,
                    func_name=f1.name,
                    arguments={},
                    cron={"cron": cron, "job_id": job1_cron.id, "now": now},
                ),
                Message,
            )
        )
        at_scheduled = ScheduledJob(
            job_id=job1.id,
            func_name=f1.name,
            message=raw_msg1,
            status=JobStatus.SCHEDULED,
        )
        cron_scheduled = ScheduledJob(
            job_id=job1_cron.id,
            func_name=f1.name,
            message=raw_msg2,
            status=JobStatus.SCHEDULED,
        )
        assert await app.configs.storage.get_schedules() == [
            at_scheduled,
            cron_scheduled,
        ]
        await job1.wait()
        await job2.wait()
        assert job1.result() == "test"
        assert job2.result() == "test"
        assert await app.configs.storage.get_schedules() == [cron_scheduled]
