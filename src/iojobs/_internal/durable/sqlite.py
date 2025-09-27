from iojobs._internal.durable.abc import JobRepository, PersistedJob


class SQLiteJobRepository(JobRepository):
    def load(self, persisted_job: PersistedJob) -> None:
        raise NotImplementedError
