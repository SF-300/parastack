from dataclasses import dataclass
from types import SimpleNamespace
from typing import (
    Protocol,
    TypeAlias,
    Generator,
    Collection,
    Sequence,
    Mapping,
    Any,
    TYPE_CHECKING,
    ParamSpec,
    TypeVar,
    Callable,
)

__all__ = "WaitCancelled", "wait", "wait_through", "NamespaceMeta", "emits"

_P = ParamSpec("_P")
_R = TypeVar("_R")
_E = TypeVar("_E")


@dataclass(frozen=True)
class WaitCancelled(RuntimeError):
    event: object


class _CondFunc(Protocol):
    def __call__(self, event: object) -> bool:
        ...


def _never(event: object) -> bool:
    return False


def _any(event: object) -> bool:
    return True


def _passthrough(event: object) -> object:
    return event


_Cond: TypeAlias = _CondFunc | Collection[type]


def _cond_matches(cond: _Cond, event: object) -> bool:
    if callable(cond) and cond(event):
        return True
    if isinstance(cond, Collection) and isinstance(event, tuple(cond)):
        return True
    return False


_WaitGen: TypeAlias = Generator[None, object, object]


class _Wait(Protocol):
    def __call__(self, until: _Cond = ..., cancel_if: _Cond = _never) -> _WaitGen:
        ...


class _Through(Protocol):
    def __call__(self, event: object, /) -> object | None:
        ...


def wait(until: _Cond = _any, cancel_if: _Cond = _never) -> _WaitGen:
    return _wait(until, cancel_if)


def wait_through(f: _Through) -> _Wait:
    def skip_wrapper(until: _Cond = _any, cancel_if: _Cond = _never) -> _WaitGen:
        return _wait(until, cancel_if, f)

    return skip_wrapper


def _wait(until: _Cond = _any, cancel_if: _Cond = _never, through: _Through = _passthrough) -> _WaitGen:
    while True:
        event = yield
        if through(event) != event:
            continue
        if _cond_matches(cancel_if, event):
            raise WaitCancelled(event)
        if _cond_matches(until, event):
            return event


class NamespaceMeta(type):
    def __new__(mcs, name: str, bases: Sequence[type], namespace: Mapping[str, Any]) -> SimpleNamespace:
        return SimpleNamespace(**namespace)


# NOTE(zeronineseven): These annotations are not strictly correct but that's enough to make
#                      autocompletion in PyCharm work in useful manner.
if TYPE_CHECKING:
    class _EventEmittingCallable(Protocol[_E, _P, _R]):
        events: _E

        def __getattr__(self, item: str) -> Any:
            ...

        def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _R:
            ...

    def emits(namespace: _E) -> Callable[[Callable[_P, _R]], _EventEmittingCallable[_E, _P, _R]]:
        ...
else:
    def emits(namespace):
        def attach(func):
            assert not hasattr(func, "emits")
            func.events = namespace
            return func
        return attach
