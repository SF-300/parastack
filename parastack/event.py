import dataclasses
import typing
from dataclasses import dataclass
from typing import Generic, Callable, ParamSpec, TypeVar, TYPE_CHECKING

from typing_extensions import Concatenate

__all__ = "Event", "MonitorEntered", "MonitorExited", "MethodCalled"


_P = ParamSpec("_P")
_R = TypeVar("_R")
_M = TypeVar("_M", bound="Monitor")


@dataclass(kw_only=True)
class Event:
    monitor: _M
    handled: bool


@typing.final
@dataclass(kw_only=True)
class MonitorEntered(Event):
    pass


@typing.final
@dataclass(kw_only=True)
class MonitorExited(Exception, Event):
    pass


@typing.final
@dataclass(kw_only=True)
class MethodCalled(Generic[_M, _P, _R], Event):
    if TYPE_CHECKING:
        monitor: _M
    args: _P.args
    kwargs: _P.kwargs
    method: Callable[Concatenate[_M, _P], _R]

    def __call__(self) -> _R:
        return self.method(self.monitor, *self.args, **self.kwargs)

    def __eq__(self, other):
        from parastack.context import EventDescriptor, EventProducer

        if isinstance(other, EventDescriptor):
            # noinspection PyProtectedMember
            return self.method == other._impl
        elif isinstance(other, EventProducer):
            # noinspection PyProtectedMember
            return self.method == other._impl and self.monitor == other._instance
        elif isinstance(other, MethodCalled):
            return dataclasses.astuple(self) == dataclasses.astuple(other)
        else:
            return super().__eq__(other)
