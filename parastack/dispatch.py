import contextlib
import functools
import typing
import weakref
import inspect
from typing import (
    Union, Iterable, TypeAlias, Callable, Dict, Generator, ParamSpec, TypeVar, Type, Collection, NewType, Iterator,
)

from parastack._logging import LoggerLike
from parastack.event import Event, MonitorEntered, MonitorExited, MethodCalled
from parastack.context import Monitor

_P = ParamSpec("_P")
_R = TypeVar("_R")

_EventSubclass: TypeAlias = TypeVar("_EventSubclass", bound=Event)
_MonitorHandler: TypeAlias = Callable[[_EventSubclass], None]
_MonitorId = NewType("_MonitorId", int)


class HandlerDone(RuntimeError):
    pass


class _MonitorDeallocated(RuntimeError):
    pass


_MonitorGenHandler: TypeAlias = Generator[None, _EventSubclass, None]
_MonitorFuncHandler: TypeAlias = Callable[[_EventSubclass], None]
_MonitorAnyHandler: TypeAlias = Union[_MonitorGenHandler, _MonitorFuncHandler]


class _MonitorContext:
    def __init__(self, mid: _MonitorId, handler: _MonitorAnyHandler, logger: LoggerLike) -> None:
        self.__mid = mid

        if inspect.isgenerator(handler):
            if inspect.getgeneratorstate(handler) == inspect.GEN_CREATED:
                next(handler)

            def wrapper(event: MethodCalled):
                try:
                    if isinstance(event, MonitorExited) and id(event.monitor) == mid:
                        try:
                            handler.throw(event)
                        finally:
                            event.handled = True
                    else:
                        handler.send(event)
                except (HandlerDone, MonitorExited):
                    raise
                except Exception as e:
                    # NOTE(zeronineseven): When MonitorExited is thrown inside handler, in most cases it will propagate
                    #                      out of handler again and will be re-wrapped here.
                    raise HandlerDone() from e
        else:
            def wrapper(event: MethodCalled):
                try:
                    handler(event)
                except Exception as e:
                    if not isinstance(e, (HandlerDone, MonitorExited)):
                        # NOTE(zeronineseven): Swallow all exceptions raised by stateless function handler unless
                        #                      it's HandlerDone exception which explicitly signals that handler
                        #                      should not be called anymore.
                        return
                    raise e

        self.__handler = wrapper

    def __call__(self, event: MethodCalled) -> None:
        assert self.__mid in set(_monitors_ids_chain_iter(event.monitor))
        self.__handler(event)
        if isinstance(event, MonitorExited) and id(event.monitor) == self.__mid:
            # NOTE(zeronineseven): If for some reason handler has swallowed MonitorExited error, throw it here again.
            raise event


class Dispatcher:
    def __init__(self, root_handler: _MonitorHandler, logger: LoggerLike) -> None:
        self.__root_handler = root_handler
        self.__mid_to_handler: Dict[_MonitorId, _MonitorHandler] = dict()
        self.__logger = logger

    @functools.singledispatchmethod
    def spawn(self, monitor: object, handler: _MonitorAnyHandler) -> None:
        raise NotImplementedError()

    @spawn.register
    def _spawn_from_event(self, event: MonitorEntered, handler: _MonitorAnyHandler) -> None:
        assert event.handled is False
        event.handled = True
        return self.spawn(event.monitor, handler)

    @spawn.register
    def _spawn_from_monitor(self, monitor: Monitor, handler: _MonitorAnyHandler) -> None:
        # NOTE(zeronineseven): Be careful not to capture 'monitor' reference into neither
        #                      of nested closures!
        def manage():
            nonlocal monitor
            try:
                mid = typing.cast(_MonitorId, id(monitor))
                assert mid not in self.__mid_to_handler

                weakref.finalize(monitor, _exceptions_ignored(manager.throw, (
                    StopIteration, _MonitorDeallocated
                )), _MonitorDeallocated())

                del monitor

                self.__mid_to_handler[mid] = _exceptions_ignored(manager.send, StopIteration)
                try:
                    ctx = _MonitorContext(mid, handler, self.__logger)
                    try:
                        while True:
                            event = yield
                            ctx(event)
                    except _MonitorDeallocated as e:
                        self.__logger.debug("Monitor %(mid)s got deallocated.", dict(mid=mid))
                        if inspect.isgenerator(handler) and inspect.getgeneratorstate(handler) != inspect.GEN_CLOSED:
                            # NOTE(zeronineseven): Will most likely rethrow _MonitorDeallocated error.
                            handler.throw(e)
                except (HandlerDone, MonitorExited, _MonitorDeallocated):
                    pass
                finally:
                    del self.__mid_to_handler[mid]
            except Exception as e:
                self.__logger.error("Unexpected error happened inside manager!", exc_info=e)

        next(manager := manage())

    def __call__(self, event: _EventSubclass) -> None:
        def handlers_iter() -> Iterator[_MonitorHandler]:
            for mid in _monitors_ids_chain_iter(event.monitor):
                try:
                    yield self.__mid_to_handler[mid]
                except KeyError:
                    continue
            yield self.__root_handler

        for handler in handlers_iter():
            handler(event)
            if event.handled:
                return
        if not event.handled and isinstance(event, MethodCalled):
            try:
                event()
            except Exception as e:
                self.__logger.error("Fallback handler defined inside %(method)s failed!", dict(
                    method=str(event.method.__qualname__),
                ), exc_info=e)


def _monitors_ids_chain_iter(monitor: Monitor) -> Iterable[_MonitorId]:
    assert isinstance(monitor, Monitor)
    while True:
        yield typing.cast(_MonitorId, id(monitor))
        if not isinstance(monitor := monitor.parent, Monitor):
            break


def _exceptions_ignored(
    f: Callable[_P, _R],
    exc: Union[Type[Exception], Collection[Type[Exception]]] = Exception,
) -> Callable[_P, _R]:
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        with contextlib.suppress(exc):
            return f(*args, **kwargs)

    return wrapper
