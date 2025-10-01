from iojobs._internal.durable.abc import JobRepository, PersistedJob


class SQLiteJobRepository(JobRepository):
    def load_all(self) -> tuple[PersistedJob]:
        raise NotImplementedError
