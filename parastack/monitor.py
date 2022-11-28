import contextlib
import typing
import inspect
from inspect import Parameter
from types import MethodType
from typing import Union, TypeAlias, Callable, ParamSpec, TypeVar, Generic, Type, Optional, ContextManager, Tuple
from contextlib import ExitStack

import typing_extensions
from typing_extensions import Self, Protocol

from parastack.event import Event, MonitorEntered, MonitorExited, MethodCalled

__all__ = "EventDescriptor", "EventProducer", "Monitor", "MonitorContext", "VoidMonitor"

_P = ParamSpec("_P")
_R = TypeVar("_R")
_M = TypeVar("_M", bound=Union["Monitor", "VoidMonitor"])


@typing.final
class EventProducer(Generic[_M, _P, _R]):
    def __init__(self, impl: Callable[_P, _R], instance: _M) -> None:
        self._impl = impl
        self._instance = instance

    def __call__(self, *args, **kwargs) -> None:
        assert len(args) == 0, "Event-producing methods MUST be called only using keyword arguments."
        # noinspection PyProtectedMember
        handler = self._instance._handler
        if handler is None:
            self._impl(self._instance, **kwargs)
        else:
            handler(MethodCalled(monitor=self._instance, kwargs=kwargs, method=self._impl, handled=False))

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
        if __debug__ is True:
            pkinds = {p.kind for p in inspect.signature(impl).parameters.values()}
            if len(pkinds.intersection({Parameter.POSITIONAL_ONLY, Parameter.VAR_POSITIONAL})) > 0:
                raise AssertionError("Event-producing methods MUST be called only using keyword arguments.")
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


@typing_extensions.runtime_checkable
class _Handler(Protocol):
    def __call__(self, event: Event) -> None:
        ...


MonitorContext: TypeAlias = Union["Monitor", "_Handler", None]


class _MonitorType(type):
    def __new__(mcs, name, bases, class_dict):
        cls = super().__new__(mcs, name, bases, {
            k: v if k.startswith("_") or not callable(v) else EventDescriptor(v)
            for k, v in class_dict.items()
        })
        return cls

    @contextlib.contextmanager
    def __call__(cls, context: MonitorContext = None, **kwargs) -> ContextManager[_M]:
        _, handler = _destructure(context)
        with ExitStack() as deffer:
            monitor = deffer.enter_context(super().__call__(context))
            if handler is not None:
                deffer.push(lambda exc_type, exc_val, tb: handler(MonitorExited(monitor=monitor, handled=False)))
                handler(MonitorEntered(monitor=monitor, handled=False, kwargs=kwargs))
            yield monitor

    def __subclasscheck__(self, subclass):
        if issubclass(subclass, VoidMonitor):
            return True
        return super().__subclasscheck__(subclass)

    def __instancecheck__(self, instance):
        if isinstance(instance, VoidMonitor):
            return True
        return super().__instancecheck__(instance)


class Monitor(metaclass=_MonitorType):
    def __init__(self, context: MonitorContext = None) -> None:
        self._parent, self._handler = _destructure(context)

    @typing.final
    def __getattr__(self, item: str):
        try:
            return getattr(self._handler, item)
        except AttributeError as e:
            raise AttributeError(f"'{type(self)}' object has no attribute '{item}'") from e

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass


@typing.final
class VoidMonitor:
    def __getattr__(self, item: str):
        if item.startswith("_"):
            raise AttributeError(item)

        def impl(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            pass

        # TODO(zeronineseven): Synthesize other metadata?
        impl.__name__ = item
        return EventProducer(impl, self)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def parent(self) -> None:
        return None


def _destructure(context: MonitorContext) -> Tuple[Optional[Monitor], Optional[_Handler]]:
    if isinstance(context, Monitor):
        # noinspection PyProtectedMember
        parent, handler = context, context._handler
    elif isinstance(context, _Handler):
        parent, handler = None, context
    elif context is None:
        parent, handler = None, None
    else:
        raise TypeError()
    return parent, handler
