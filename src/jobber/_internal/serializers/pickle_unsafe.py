# ruff: noqa: ERA001
# pyright: reportExplicitAny=false

import pickle  # nosec B403

from typing_extensions import override

from jobber._internal.serializers.base import JobsSerializer, SerializableTypes


class UnsafePickleSerializer(JobsSerializer):
    @override
    def dumpb(self, data: SerializableTypes) -> bytes:
        # nosemgrep: python.lang.security.deserialization.pickle.avoid-pickle
        return pickle.dumps(data)

    @override
    def loadb(self, data: bytes) -> SerializableTypes:
        # nosemgrep: python.lang.security.deserialization.pickle.avoid-pickle
        decoded: SerializableTypes = pickle.loads(data)  # noqa: S301 # nosec B301
        return decoded
