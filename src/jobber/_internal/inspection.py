import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Generic, ParamSpec, TypeVar, get_type_hints

ReturnT = TypeVar("ReturnT")
ParamsT = ParamSpec("ParamsT")


@dataclass(slots=True, kw_only=True)
class FieldIn:
    name: str
    type_hint: Any


@dataclass(slots=True, kw_only=True)
class FuncSpec(Generic[ReturnT]):
    name: str
    fields_in: list[FieldIn]
    signature: inspect.Signature
    result_type: type[ReturnT]


def get_func_fields(
    sig: inspect.Signature,
    hints: dict[str, Any],
) -> list[FieldIn]:
    return [
        FieldIn(
            name=arg.name,
            type_hint=hints.get(arg.name, Any),
        )
        for arg in sig.parameters.values()
    ]


def get_result_type(hints: dict[str, Any]) -> Any:  # noqa: ANN401
    return hints.get("return", Any)


def make_func_spec(func: Callable[ParamsT, ReturnT]) -> FuncSpec[ReturnT]:
    sig = inspect.signature(func)
    hints = get_type_hints(func)
    fields_in = get_func_fields(sig, hints)
    return FuncSpec(
        name=func.__name__,
        fields_in=fields_in,
        result_type=get_result_type(hints),
        signature=sig,
    )
