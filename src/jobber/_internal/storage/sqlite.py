from typing_extensions import override

from jobber._internal.storage.abc import JobRepository, JobStored


class SQLiteJobRepository(JobRepository):
    @override
    def load_all(self) -> tuple[JobStored]:
        raise NotImplementedError
