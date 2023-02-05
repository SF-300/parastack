import dataclasses
import typing
from dataclasses import dataclass, field
from types import MethodType
from typing import Generic, Callable, ParamSpec, TypeVar, Optional

from typing_extensions import Concatenate

__all__ = "Event", "MonitorEntered", "MonitorExited", "MonitorMethodCalled"


_P = ParamSpec("_P")
_R = TypeVar("_R")
_M = TypeVar("_M", bound="Monitor")


@dataclass
class Event(Generic[_M, _P, _R]):
    monitor: _M
    function: Callable[Concatenate[_M, _P], _R]
    args: _P.args = tuple()
    kwargs: _P.kwargs = field(default_factory=dict)
    handled: bool = False

    def __eq__(self, other):
        from parastack.monitor import _EventDescriptor, _EventProducer

        if isinstance(other, _EventDescriptor):
            # noinspection PyProtectedMember
            return self.function == other._func
        if isinstance(other, _EventProducer):
            # noinspection PyProtectedMember
            return MethodType(self.monitor, self.function) == other._meth
        if isinstance(other, MonitorMethodCalled):
            return dataclasses.astuple(self) == dataclasses.astuple(other)
        if self.function == other:
            return True
        return super().__eq__(other)


@typing.final
@dataclass
class MonitorEntered(Event[_M, _P, _R]):
    def __str__(self) -> str:
        return f"{str(self.monitor)} entered event."


@typing.final
@dataclass
class MonitorExited(Exception, Event[_M, _P, _R]):
    @property
    def cause(self) -> Optional[Exception]:
        return self.__cause__

    def __str__(self) -> str:
        return f"{str(self.monitor)} exited event."


@typing.final
@dataclass
class MonitorMethodCalled(Event[_M, _P, _R]):
    def __call__(self, *args, **kwargs) -> _R:
        if self.handled:
            raise AssertionError("Event has already been handled!")
        try:
            return self.function(self.monitor, *(*args, *self.args[len(args):]), **{**self.kwargs, **kwargs})
        finally:
            self.handled = True

    def __str__(self) -> str:
        return f"{str(self.function)} called event."
