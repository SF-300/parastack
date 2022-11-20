from typing import Protocol, TypeAlias, Generator

from parastack.event import Event

__all__ = "skip", "skip_through"


class _Until(Protocol):
    def __call__(self, event: Event) -> bool:
        ...


_SkipGen: TypeAlias = Generator[None, Event, Event]


class _Skip(Protocol):
    def __call__(self, until: _Until) -> _SkipGen:
        ...


class _Through(Protocol):
    def __call__(self, event: Event, /) -> Event | None:
        ...


def skip(until: _Until) -> _SkipGen:
    return _skip(until)


def skip_through(f: _Through) -> _Skip:
    def skip_wrapper(until: _Until) -> _SkipGen:
        return _skip(until, f)

    return skip_wrapper


def _skip(until: _Until, through: _Through = lambda e: e) -> _SkipGen:
    while True:
        event = yield
        if through(event) is None:
            event.handled = True
            continue
        if until(event):
            event.handled = True
            return event
