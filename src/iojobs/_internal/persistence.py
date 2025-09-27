import sqlite3
from dataclasses import dataclass
from typing import Final

from iojobs._internal.enums import ExecutionMode, JobStatus


@dataclass(slots=True)
class PersistedJob:
    job_id: str
    func_id: str
    exec_at_timestamp: float
    status: JobStatus
    func_args: bytes
    func_kwargs: bytes
    execution_mode: ExecutionMode
    cron_expression: str | None = None
    result: bytes | None = None
    error: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0


class DurableSQLite:
    def __init__(self, db_path: str = ".") -> None:
        db_path += "iojobs"
        self._con: Final = sqlite3.connect(db_path)
