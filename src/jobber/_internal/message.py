from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from jobber._internal.configuration import Cron
    from jobber._internal.serializers.base import JobsSerializer
    from jobber._internal.typeadapter.base import Dumper, Loader


@dataclass(slots=True, kw_only=True)
class Message:
    route_name: str
    job_id: str
    args: list[Any]
    kwargs: dict[str, Any]
    options: dict[str, Any]
    cron: Cron | None = None
    at_timestamp: float | None = None


def pack_message(
    message: Message,
    /,
    dumper: Dumper,
    serialzier: JobsSerializer,
) -> bytes:
    return serialzier.dumpb(dumper.dump(message, Message))


def unpack_message(
    raw_message: bytes,
    /,
    loader: Loader,
    serialzier: JobsSerializer,
) -> Message:
    return loader.load(serialzier.loadb(raw_message), Message)
