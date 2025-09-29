from typing import TypeAlias

import pytest

from iojobs._internal.serializers import (
    AstLiteralSerializer,
    IOJobsSerializer,
    UnsafePickleSerializer,
)

AllowedDataTypes: TypeAlias = (
    None
    | bool
    | int
    | float
    | str
    | bytes
    | list["AllowedDataTypes"]
    | tuple["AllowedDataTypes"]
    | dict[str, "AllowedDataTypes"]
)


@pytest.mark.parametrize(
    "serializer",
    [AstLiteralSerializer(), UnsafePickleSerializer()],
)
@pytest.mark.parametrize(
    "data",
    [
        None,
        True,
        False,
        123,
        123.45,
        "hello",
        b"world",
        [1, "a", None, [2, "b", True]],
        (1, "a", None, (2, "b", True)),
        {"a": 1, "b": None},
        {1, "a", None},
    ],
)
def test_serialization_all(
    serializer: IOJobsSerializer,
    data: AllowedDataTypes,
) -> None:
    """Tests that all serializers can [de]serialize basic Python types."""
    serialized = serializer.dumpb(data)
    deserialized = serializer.loadb(serialized)
    assert deserialized == data
