import json

from typing_extensions import override

from jobber._internal.serializers.base import (
    JobsSerializer,
    JsonDecoderHook,
    SerializableTypes,
    TypeRegistry,
    json_extended_encoder,
)


class ExtendedEncoder(json.JSONEncoder):
    @override
    def encode(self, o: SerializableTypes) -> str:
        json_compat = json_extended_encoder(o)
        return super().encode(json_compat)


class JSONSerializer(JobsSerializer):
    def __init__(self, registry: TypeRegistry) -> None:
        self.decoder_hook: JsonDecoderHook = JsonDecoderHook(registry)

    @override
    def dumpb(self, data: SerializableTypes) -> bytes:
        return json.dumps(data, cls=ExtendedEncoder).encode("utf-8")

    @override
    def loadb(self, data: bytes) -> SerializableTypes:
        decoded: SerializableTypes = json.loads(
            data,
            object_hook=self.decoder_hook,
        )
        return decoded
