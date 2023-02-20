from types import SimpleNamespace
from typing import Protocol, TypeAlias, Generator, Collection, Sequence, Mapping, Any

__all__ = "wait", "wait_through", "NamespaceMeta"


class _UntilFunc(Protocol):
    def __call__(self, event: object) -> bool:
        ...


_UntilCond: TypeAlias = _UntilFunc | Collection[type] | None


_WaitGen: TypeAlias = Generator[None, object, object]


class _Wait(Protocol):
    def __call__(self, until: _UntilCond = ...) -> _WaitGen:
        ...


class _Through(Protocol):
    def __call__(self, event: object, /) -> object | None:
        ...


def wait(until: _UntilCond = None) -> _WaitGen:
    return _wait(until)


def wait_through(f: _Through) -> _Wait:
    def skip_wrapper(until: _UntilCond = None) -> _WaitGen:
        return _wait(until, f)

    return skip_wrapper


def _wait(until: _UntilCond = None, through: _Through = lambda e: e) -> _WaitGen:
    while True:
        event = yield
        if through(event) is None:
            continue
        if until is None:
            # NOTE(zeronineseven): Match everything.
            return event
        if callable(until) and until(event):
            return event
        if isinstance(until, Collection) and any(isinstance(event, t) for t in until):
            return event


class NamespaceMeta(type):
    def __new__(mcs, name: str, bases: Sequence[type], namespace: Mapping[str, Any]) -> SimpleNamespace:
        return SimpleNamespace(**namespace)
