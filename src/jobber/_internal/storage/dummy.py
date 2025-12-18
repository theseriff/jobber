from typing_extensions import override

from jobber._internal.storage.abc import JobRepository, JobStored


class DummyRepository(JobRepository):
    @override
    def load_all(self) -> list[JobStored]:
        return []
