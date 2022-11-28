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
from parastack.monitor import Monitor

__all__ = "HandlerDone", "Dispatcher"

_P = ParamSpec("_P")
_R = TypeVar("_R")

_EventSubclass: TypeAlias = TypeVar("_EventSubclass", bound=Event)
_MonitorHandler: TypeAlias = Callable[[_EventSubclass], None]
_MonitorId = NewType("_MonitorId", int)


class HandlerDone(RuntimeError):
    pass


class MonitorDeallocated(RuntimeError):
    pass


_GenMonitorHandler: TypeAlias = Generator[None, _EventSubclass, None]
_FuncMonitorHandler: TypeAlias = Callable[[_EventSubclass], None]
_AnyMonitorHandler: TypeAlias = Union[_GenMonitorHandler, _FuncMonitorHandler]


class _NormalizedMonitorHandler:
    def __init__(self, handler: _AnyMonitorHandler, mid: _MonitorId, logger: LoggerLike) -> None:
        if inspect.isgenerator(handler):
            if inspect.getgeneratorstate(handler) == inspect.GEN_CREATED:
                next(handler)

            def send(event: MethodCalled) -> None:
                try:
                    if isinstance(event, MonitorExited) and id(event.monitor) == mid:
                        try:
                            handler.throw(event)
                        finally:
                            event.handled = True
                    else:
                        handler.send(event)
                except (HandlerDone, MonitorExited) as e:
                    raise e
                except Exception as e:
                    # NOTE(zeronineseven): When MonitorExited is thrown inside handler, in most cases it will propagate
                    #                      out of handler again and will be re-wrapped here.
                    raise HandlerDone() from e

            throw = handler.throw
        else:
            def send(event: MethodCalled) -> None:
                try:
                    handler(event)
                except Exception as e:
                    if not isinstance(e, (HandlerDone, MonitorExited)):
                        # NOTE(zeronineseven): Swallow all exceptions raised by stateless function handler unless
                        #                      it's HandlerDone exception which explicitly signals that handler
                        #                      should not be called anymore.
                        logger.debug("Ignoring exception raised by function handler for '%(mid)s' monitor.", exc_info=e)
                        return
                    raise e

            def throw(_: Exception) -> None:
                pass

        self.send, self.throw = send, throw


class Dispatcher:
    def __init__(self, root_handler: Callable[[Event], Event | None], logger: LoggerLike) -> None:
        def handler_wrapper(event):
            if root_handler(event) is None:
                event.handled = True

        self.__root_handler = handler_wrapper
        self.__mid_to_handler: Dict[_MonitorId, _MonitorHandler] = dict()
        self.__logger = logger

    @functools.singledispatchmethod
    def spawn(self, monitor: object, handler: _AnyMonitorHandler) -> None:
        raise NotImplementedError()

    @spawn.register
    def _spawn_from_event(self, event: MonitorEntered, handler: _AnyMonitorHandler) -> None:
        assert event.handled is False
        event.handled = True
        return self._spawn_from_monitor(event.monitor, handler)

    @spawn.register
    def _spawn_from_monitor(self, monitor: Monitor, handler: _AnyMonitorHandler) -> None:
        # NOTE(zeronineseven): Be careful not to capture 'monitor' reference into neither
        #                      of nested closures!
        def manage():
            nonlocal monitor, handler
            mid = typing.cast(_MonitorId, id(monitor))
            assert mid not in self.__mid_to_handler

            # NOTE(zeronineseven): Deallocation-signaling exception is thrown into 'manager' coroutine to not
            #                      give handlers a chance to silently swallow it -
            #                      they get merely notified about it.
            weakref.finalize(monitor, _exceptions_ignored(
                manager.throw, (StopIteration, MonitorDeallocated),
            ), MonitorDeallocated())

            del monitor

            self.__mid_to_handler[mid] = _exceptions_ignored(manager.send, StopIteration)
            try:
                handler = _NormalizedMonitorHandler(handler, mid, self.__logger)
                try:
                    while True:
                        event = yield
                        handler.send(event)
                        if isinstance(event, MonitorExited) and id(event.monitor) == mid:
                            # NOTE(zeronineseven): If for some reason handler has swallowed MonitorExited error,
                            #                      re-throw it here again.
                            self.__logger.debug(
                                f"Handler for '{mid}' monitor id has swallowed "
                                f"'{type(MonitorExited)}' event - re-raising..."
                            )
                            raise event
                except MonitorDeallocated as e:
                    self.__logger.debug(f"Monitor with id '{mid}' got deallocated.")
                    # NOTE(zeronineseven): Will most likely rethrow MonitorDeallocated error.
                    handler.throw(e)
            except (HandlerDone, MonitorExited, MonitorDeallocated):
                pass
            except Exception as processing_error:
                self.__logger.error("Unexpected error happened inside manager!", exc_info=processing_error)
            finally:
                del self.__mid_to_handler[mid]

        try:
            next(manager := manage())
        except Exception as boostrap_error:
            self.__logger.error("Failed to bootstrap 'manager' coroutine!", exc_info=boostrap_error)

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
                self.__logger.error(
                    f"Fallback handler defined inside '{event.method.__qualname__}' failed!",
                    exc_info=e,
                )


def _monitors_ids_chain_iter(monitor: Monitor) -> Iterable[_MonitorId]:
    assert isinstance(monitor, Monitor)
    while True:
        yield typing.cast(_MonitorId, id(monitor))
        # noinspection PyProtectedMember
        if not isinstance(monitor := monitor._parent, Monitor):
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
