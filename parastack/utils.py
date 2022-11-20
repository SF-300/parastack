from typing import Protocol, TypeAlias, Generator

from parastack.event import Event

__all__ = "wait", "wait_through"


class _Until(Protocol):
    def __call__(self, event: Event) -> bool:
        ...


_WaitGen: TypeAlias = Generator[None, Event, Event]


class _Wait(Protocol):
    def __call__(self, until: _Until) -> _WaitGen:
        ...


class _Through(Protocol):
    def __call__(self, event: Event, /) -> Event | None:
        ...


def wait(until: _Until) -> _WaitGen:
    return _wait(until)


def wait_through(f: _Through) -> _Wait:
    def skip_wrapper(until: _Until) -> _WaitGen:
        return _wait(until, f)

    return skip_wrapper


def _wait(until: _Until, through: _Through = lambda e: e) -> _WaitGen:
    while True:
        event = yield
        if through(event) is None:
            event.handled = True
            continue
        if until(event):
            event.handled = True
            return event
