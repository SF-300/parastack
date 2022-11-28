import dataclasses
import typing
from dataclasses import dataclass
from typing import Generic, Callable, ParamSpec, TypeVar, Optional

from typing_extensions import Concatenate

__all__ = "Event", "MonitorEntered", "MonitorExited", "MethodCalled"


_P = ParamSpec("_P")
_R = TypeVar("_R")
_M = TypeVar("_M", bound="Monitor")


@dataclass(kw_only=True)
class Event(Generic[_M]):
    monitor: _M
    handled: bool


@typing.final
@dataclass(kw_only=True)
class MonitorEntered(Event[_M], Generic[_M, _P]):
    kwargs: _P.kwargs


@typing.final
@dataclass(kw_only=True)
class MonitorExited(Exception, Event[_M]):
    @property
    def cause(self) -> Optional[Exception]:
        return self.__cause__


@typing.final
@dataclass(kw_only=True)
class MethodCalled(Event[_M], Generic[_M, _P, _R]):
    kwargs: _P.kwargs
    method: Callable[Concatenate[_M, _P], _R]

    def __call__(self) -> _R:
        return self.method(self.monitor, **self.kwargs)

    def __eq__(self, other):
        from parastack.monitor import EventDescriptor, EventProducer

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
