from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Generic, TypeAlias, TypeVar

AnyCronParser: TypeAlias = "CronParser[Any]"
T = TypeVar("T", bound="AnyCronParser")


class CronParser(ABC, Generic[T]):
    @classmethod
    @abstractmethod
    def create(cls, expression: str) -> T:
        raise NotImplementedError

    @abstractmethod
    def next_run(self, *, now: datetime) -> datetime:
        raise NotImplementedError

    @abstractmethod
    def get_expression(self) -> str:
        raise NotImplementedError
