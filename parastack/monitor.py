import typing
from types import MethodType

import typing_extensions
from typing import Union, TypeAlias, Callable, ParamSpec, TypeVar, Generic, Type, Optional

from typing_extensions import Self, Protocol

from parastack.event import Event, MonitorEntered, MonitorExited, MethodCalled

__all__ = "EventDescriptor", "EventProducer", "Monitor", "MonitorParent"

_P = ParamSpec("_P")
_R = TypeVar("_R")
_M = TypeVar("_M", bound="Monitor")


@typing.final
class EventProducer(Generic[_M, _P, _R]):
    def __init__(self, impl: Callable[_P, _R], instance: _M) -> None:
        self._impl = impl
        self._instance = instance

    def __call__(self, *args, **kwargs) -> None:
        # NOTE(zeronineseven): Perform manual de-mangling as we don't want the
        #                      inheritors to have access to this field.
        handler = getattr(self._instance, f"_{Monitor.__name__}__handler", None)
        if handler is None:
            self._impl(self._instance, *args, **kwargs)
        else:
            handler(MethodCalled(monitor=self._instance, args=args, kwargs=kwargs, method=self._impl, handled=False))

    def __eq__(self, other) -> bool:
        if isinstance(other, EventDescriptor):
            # noinspection PyProtectedMember
            return self._impl == other._impl
        elif isinstance(other, EventProducer):
            return self._impl == other._impl and self._instance == other._instance
        elif isinstance(other, MethodCalled):
            return self._impl == other.method and self._instance == other.monitor
        else:
            return super().__eq__(other)

    def __hash__(self):
        return hash(MethodType(self._impl, self._instance))


@typing.final
class EventDescriptor(Generic[_P, _R]):
    def __init__(self, impl: Callable[_P, _R]) -> None:
        self._impl = impl

    def __get__(self, instance: Optional[_M], owner: Type["Monitor"]) -> Union[EventProducer[_M, _P, _R], Self]:
        return self if instance is None else EventProducer(self._impl, instance)

    def __eq__(self, other) -> bool:
        if isinstance(other, (EventDescriptor, EventProducer)):
            # noinspection PyProtectedMember
            return self._impl == other._impl
        elif isinstance(other, MethodCalled):
            return self._impl == other.method
        else:
            return super().__eq__(other)

    def __hash__(self):
        return id(self._impl)


class _MonitorUsageEnforcer:
    def __init__(self, monitor: "Monitor"):
        self._monitor = monitor
        self._entered = False

    def __getattr__(self, item: str):
        if not self._entered:
            raise AssertionError("Monitor must be entered first!")
        return getattr(self._monitor, item)

    def __enter__(self):
        if self._entered:
            raise AssertionError("Monitor can be entered only once!")
        self._entered = True
        return self._monitor.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._entered:
            raise AssertionError("Monitor has not been entered!")
        return self._monitor.__exit__(exc_type, exc_val, exc_tb)


class _MonitorType(type):
    def __new__(mcs, name, bases, class_dict):
        cls = super().__new__(
            mcs, name, bases, {
                k: v if k.startswith("_") or not callable(v) else EventDescriptor(v)
                for k, v in class_dict.items()
            }
        )
        return cls

    def __call__(cls, *args, **kwargs) -> _M:
        monitor = super().__call__(*args, **kwargs)
        if __debug__ is True:
            monitor = _MonitorUsageEnforcer(monitor)
        return monitor

    def __subclasscheck__(self, subclass):
        if issubclass(subclass, _MonitorUsageEnforcer):
            return True
        return super().__subclasscheck__(subclass)

    def __instancecheck__(self, instance):
        if isinstance(instance, _MonitorUsageEnforcer):
            return True
        return super().__instancecheck__(instance)


@typing_extensions.runtime_checkable
class _Handler(Protocol):
    def __call__(self, event: Event) -> None:
        ...


MonitorParent: TypeAlias = Union["Monitor", "_Handler", None]


class Monitor(metaclass=_MonitorType):
    def __init__(self, parent: MonitorParent = None) -> None:
        if isinstance(parent, Monitor):
            self.__parent, self.__handler = parent, parent.__handler
        elif isinstance(parent, _Handler):
            self.__parent, self.__handler = None, parent
        elif parent is None:
            self.__parent, self.__handler = None, None
        else:
            raise TypeError()

    def __enter__(self) -> Self:
        if self.__handler is not None:
            self.__handler(MonitorEntered(monitor=self, handled=False))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.__handler is not None:
            e = MonitorExited(monitor=self, handled=False)
            if exc_val is not None:
                e.__cause__ = exc_val
            self.__handler(e)

    @property
    def parent(self) -> "Monitor":
        return self.__parent
