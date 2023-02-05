import functools
import typing
from types import MethodType
from typing import Union, TypeAlias, Callable, ParamSpec, TypeVar, Generic, Type, Optional, Final

import typing_extensions
from typing_extensions import Self, Protocol

from parastack.event import Event, MonitorEntered, MonitorExited, MonitorMethodCalled

__all__ = "_EventDescriptor", "_EventProducer", "Monitor", "MonitorContext", "VoidMonitor"

_P = ParamSpec("_P")
_R = TypeVar("_R")
_M = TypeVar("_M", bound=Union["Monitor", "VoidMonitor"])


class _EventProducer(Generic[_M, _P, _R]):
    def __init__(self, func: Callable[_P, _R], monitor: _M) -> None:
        self._func = func
        self._monitor = monitor

    @functools.cached_property
    def _meth(self) -> MethodType:
        return MethodType(self._func, self._monitor)

    def __call__(self, *args, **kwargs):
        # noinspection PyProtectedMember
        handler = self._monitor._handler
        if handler is None:
            self._meth(*args, **kwargs)
        else:
            handler(MonitorMethodCalled(monitor=self._monitor, args=args, kwargs=kwargs, function=self._func))

    def __eq__(self, other) -> bool:
        if isinstance(other, _EventProducer):
            return self._meth == other._meth
        if isinstance(other, _EventDescriptor):
            # noinspection PyProtectedMember
            return self._func == other._func
        if isinstance(other, Event):
            return self._meth == MethodType(other.function, other.monitor)
        if self._func == other:
            return True
        return super().__eq__(other)

    def __hash__(self):
        return hash(self._meth)


class _CmEnterEventProducer(_EventProducer):
    def __call__(self):
        func, monitor = self._func, self._monitor
        # noinspection PyProtectedMember
        handler = monitor._handler
        try:
            return func(monitor)
        finally:
            if handler is None:
                return
            # noinspection PyProtectedMember
            handler(MonitorEntered(
                function=func,
                monitor=monitor,
                args=monitor._args,
                kwargs=monitor._kwargs,
            ))
            # noinspection PyProtectedMember
            del monitor._args, monitor._kwargs


class _CmExitEventProducer(_EventProducer):
    def __call__(self, *args, **kwargs):
        func, monitor = self._func, self._monitor
        # noinspection PyProtectedMember
        handler = monitor._handler
        try:
            return func(monitor, *args, **kwargs)
        finally:
            if handler is None:
                return
            handler(MonitorExited(
                function=func,
                monitor=monitor,
            ))


@typing.final
class _EventDescriptor(Generic[_M, _P, _R]):
    def __init__(self, func: Callable[_P, _R], event_producer_cls: Type[_EventProducer]) -> None:
        self._func = func
        self._event_procuder_cls = event_producer_cls

    def __get__(self, instance: Optional[_M], owner: Type["Monitor"]) -> Union[_EventProducer[_M, _P, _R], Self]:
        return self if instance is None else self._event_procuder_cls(self._func, instance)

    def __call__(self, *args, **kwargs):
        return self._event_procuder_cls(self._func, args[0])(*args[1:], **kwargs)

    def __eq__(self, other) -> bool:
        if isinstance(other, _EventDescriptor):
            return self._func == other._func
        if isinstance(other, _EventProducer):
            # noinspection PyProtectedMember
            return self._func == other._meth.__func__
        if isinstance(other, Event):
            return self._func == other.function
        return super().__eq__(other)

    def __hash__(self):
        return id(self._func)


@typing_extensions.runtime_checkable
class _Handler(Protocol):
    def __call__(self, event: Event) -> None:
        ...


MonitorContext: TypeAlias = Union["Monitor", "_Handler", None]


class _MonitorType(type):
    def __new__(mcs, name, bases, class_dict):
        namespace = {
            name: func if name.startswith("_") or not callable(func) else _EventDescriptor(func, _EventProducer)
            for name, func in class_dict.items()
        }

        for cm_func_name, event_producer_cls, stub in (
            ("__enter__", _CmEnterEventProducer, lambda m: m),
            ("__exit__", _CmExitEventProducer, lambda m, exc_type, exc_val, exc_tb: None),
        ):
            cm_func = class_dict.get(cm_func_name, stub)
            namespace[cm_func_name] = _EventDescriptor(cm_func, event_producer_cls)

        return super().__new__(mcs, name, bases, namespace)

    def __subclasscheck__(self, subclass):
        if issubclass(subclass, VoidMonitor):
            return True
        return super().__subclasscheck__(subclass)

    def __instancecheck__(self, instance):
        if isinstance(instance, VoidMonitor):
            return True
        return super().__instancecheck__(instance)


class Monitor(metaclass=_MonitorType):
    _parent: Final[Optional["Monitor"]]
    _handler: Final[Optional[_Handler]]

    def __init__(self, context: MonitorContext, *args, **kwargs) -> None:
        if isinstance(context, Monitor):
            # noinspection PyProtectedMember
            self._parent, self._handler = context, context._handler
        elif isinstance(context, _Handler):
            self._parent, self._handler = None, context
        elif context is None:
            self._parent, self._handler = None, None
        else:
            raise TypeError()
        if self._handler is not None:
            # NOTE(zeronineseven): Will be included in MonitorEntered event.
            self._args, self._kwargs = args, kwargs

    @typing.final
    def __getattr__(self, item: str):
        # NOTE(zeronineseven): Metaclass generates descriptors for all events explicitly specified. All requests
        #                      to attributes never mentioned end up here and are passed down to self._handler.
        try:
            return getattr(self._handler, item)
        except AttributeError as e:
            raise AttributeError(f"'{type(self)}' object has no attribute '{item}'") from e

    @typing.final
    def __enter__(self) -> Self:
        return self

    @typing.final
    def __exit__(self, exc_type, exc_val, exc_tb):
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
        return _EventProducer(impl, self)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @property
    def parent(self) -> None:
        return None
