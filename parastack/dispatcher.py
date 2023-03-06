import abc
import contextlib
import itertools
import typing
import weakref
import inspect
from abc import ABC
from typing import Union, Iterable, TypeAlias, Callable, Dict, Generator, NewType, Self, Sequence
from unittest.mock import MagicMock

from parastack._logging import LoggerLike
from parastack.emitter import Emitter, Forked, Joined

__all__ = "Dispatcher"

_EmitterId = NewType("_EmitterId", int)
_EmitterHandler: TypeAlias = Callable[[_EmitterId, object], None]


class HandlerDone(Joined):
    pass


class HandlerFailed(Joined):
    pass


class EmitterDeallocated(Joined):
    pass


class DispatcherClosed(Joined):
    pass


_GenHandler: TypeAlias = Generator[None, object, None]
_GenEmitterHandler: TypeAlias = Callable[[], _GenHandler]
_FuncHandler: TypeAlias = Callable[[object], None]
_AnyHandler: TypeAlias = Union[_GenHandler, _GenEmitterHandler, _FuncHandler]


class _NormalizedHandler(ABC):
    @property
    @abc.abstractmethod
    def done(self) -> bool:
        pass

    @abc.abstractmethod
    def send(self, event: object) -> None:
        pass

    @abc.abstractmethod
    def throw(self, exc: Joined) -> None:
        pass


class _MultiHandler(_NormalizedHandler):
    def __init__(self, logger: LoggerLike, handlers: Sequence[_AnyHandler]) -> None:
        self._handlers = tuple(_normalize_handlers(logger, h) for h in handlers)

    @property
    def done(self) -> bool:
        return all(h.done for h in self._handlers)

    def send(self, event: object) -> None:
        assert not self.done
        for handler in self._handlers:
            with contextlib.suppress(Joined):
                handler.send(event)
        if self.done:
            raise HandlerDone()

    def throw(self, exc: Joined) -> None:
        assert not self.done
        for handler in self._handlers:
            with contextlib.suppress(Joined):
                handler.throw(exc)
        raise exc

    def __str__(self) -> str:
        return str(self._handlers)
    # 
    # def __repr__(self) -> str:
    #     pass


class _FuncNormalizedHandler(_NormalizedHandler):
    def __init__(self, logger: LoggerLike, handler: _FuncHandler) -> None:
        self._handler = handler
        self._logger = logger
        self._done = False

    @property
    def done(self) -> bool:
        return self._done

    def send(self, event: object) -> None:
        assert not self._done
        try:
            self._handler(event)
        except Exception as e:
            if not isinstance(e, HandlerDone):
                # NOTE(zeronineseven): Swallow all exceptions raised by stateless function handler unless
                #                      it's HandlerDone exception which explicitly signals that handler
                #                      should not be called anymore.
                self._logger.debug("Ignoring exception raised by function handler for '%(eid)s' emitter.", exc_info=e)
                return
            self._done = True
            raise e

    def throw(self, e: Joined) -> None:
        assert not self._done
        self._done = True
        raise e

    def __repr__(self) -> str:
        return f"Function '{self._handler.__qualname__}' handler" + ("done" if self._done else "suspended")


class _GenNormalizedHandler(_NormalizedHandler):
    # NOTE(zeronineseven): Generator behaviour spec:
    #                       * https://peps.python.org/pep-0342/#specification-summary

    def __init__(self, logger: LoggerLike, handler: _GenHandler) -> None:
        assert inspect.getgeneratorstate(handler) == inspect.GEN_CREATED
        # NOTE(zeronineseven): Bootstrap the generator and rewind it until the first yield.
        next(handler)

        self._handler = handler
        self._logger = logger

    @property
    def done(self) -> bool:
        return inspect.getgeneratorstate(self._handler) == inspect.GEN_CLOSED

    def send(self, event: object) -> None:
        assert not self.done
        try:
            self._handler.send(event)
        except StopIteration:
            raise HandlerDone()
        except Joined as e:
            raise e
        except Exception as e:
            raise HandlerFailed() from e

    def throw(self, exc: Joined) -> None:
        assert not self.done
        exc_type = type(exc)
        try:
            self._handler.throw(exc_type, exc, exc.__traceback__)
        except StopIteration:
            raise exc
        except Joined as e:
            if e != exc:
                self._logger.debug("Generator-based handler altered Join signal!")
            raise e
        except Exception as e:
            self._logger.debug("Generator-based handler responded with an exception to Join signal!", exc_info=e)
            raise HandlerFailed() from e
        else:
            self._logger.debug("Generator-based handler has swallowed Join signal completely!")
            raise exc

    def __repr__(self) -> str:
        # NOTE(zeronineseven): inspect.GEN_CREATED is impossible here as we're explicitly bootstrapping/rewinding
        #                      generator in the __init__ method.
        status = {
            inspect.GEN_RUNNING: "running",
            inspect.GEN_SUSPENDED: "suspended",
            inspect.GEN_CLOSED: "done",
        }[inspect.getgeneratorstate(self._handler)]
        return f"Generator '{self._handler.__qualname__}' handler ({status})"


def _normalize_handlers(logger: LoggerLike, *handlers: _AnyHandler) -> _NormalizedHandler:
    if len(handlers) == 0:
        raise ValueError("At least one handler MUST be provided!")
    if len(handlers) != 1:
        return _MultiHandler(logger, handlers)
    handler = handlers[0]
    if inspect.isgeneratorfunction(handler):
        handler = handler()
        assert inspect.isgenerator(handler)
    if inspect.isgenerator(handler):
        return _GenNormalizedHandler(logger, handler)
    else:
        return _FuncNormalizedHandler(logger, handler)


class Dispatcher:
    def __init__(self, *handlers: _AnyHandler, logger: LoggerLike = MagicMock()) -> None:
        root_handler = _normalize_handlers(logger, *handlers)

        def root_handler_wrapper(_: _EmitterId, event: object) -> None:
            try:
                root_handler.send(event)
                if isinstance(event, DispatcherClosed):
                    root_handler.throw(event)
            except DispatcherClosed:
                pass
            except Exception as e:
                logger.debug(f"Root handler failed to handle event '{event}'!", exc_info=e)

        self.__root_handler: _EmitterHandler = root_handler_wrapper
        # TODO(zeronineseven): This approach might be problematic:
        #                       * https://stackoverflow.com/questions/71755312/is-id-function-buggy
        self.__eid_to_handler: Dict[_EmitterId, _EmitterHandler] = dict()
        self.__logger = logger

    def fork(self, forked: Forked, *handlers: _AnyHandler) -> None:
        # NOTE(zeronineseven): There are 3 ways which can cause handler to be discarded:
        #                      1. Handler has received 'Joined' event (expected scenario).
        #                      2. Emitter got deallocated while handler is still alive.
        #                      3. Generator-based handler has prematurely raised some exception
        #                         while Emitter is still alive.
        eid = typing.cast(_EmitterId, id(forked.child))
        assert eid not in self.__eid_to_handler

        # NOTE(zeronineseven): Be careful not to capture 'forked' in any nested closures!
        def cleanup():
            finalizer.detach()
            del self.__eid_to_handler[eid]
            self.__logger.debug(f"Handler for emitter with id '{eid}' got cleaned up.")

        handler = _normalize_handlers(self.__logger, *handlers)

        def handler_wrapper(sender_eid: _EmitterId, event: object):
            try:
                handler.send(event)
                if isinstance(event, Joined) and sender_eid == eid:
                    # NOTE(zeronineseven): If we're here, it means that it was this handler, which has been joined, but
                    #                      for some reason it decided to ignore own termination.
                    handler.throw(event)
            except Joined:
                cleanup()

        def on_deallocated():
            self.__logger.debug(f"Emitter with id '{eid}' got deallocated.")
            try:
                handler.throw(EmitterDeallocated())
            except Joined:
                pass
            cleanup()

        finalizer = weakref.finalize(forked.child, on_deallocated)

        self.__eid_to_handler[eid] = handler_wrapper

    def __call__(self, emitter: Emitter, event: object) -> None:
        sender_eid = typing.cast(_EmitterId, id(emitter))

        def find_handler() -> _EmitterHandler:
            for eid in _emitters_ids_chain_iter(emitter):
                try:
                    return self.__eid_to_handler[eid]
                except KeyError:
                    continue
            return self.__root_handler

        find_handler()(sender_eid, event)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        joined_event = DispatcherClosed()
        if exc_val is not None:
            joined_event.__cause__ = exc_val
        for eid, handler in itertools.chain(
            self.__eid_to_handler.items(),
            # NOTE(zeronineseven): 0 is just some dummy placeholder.
            (
                (typing.cast(_EmitterId, 0), self.__root_handler),
            ),
        ):
            with contextlib.suppress(Joined):
                handler(eid, joined_event)


def _emitters_ids_chain_iter(emitter: Emitter) -> Iterable[_EmitterId]:
    assert isinstance(emitter, Emitter)
    while True:
        yield typing.cast(_EmitterId, id(emitter))
        emitter = getattr(emitter, "_parent", None)
        if not isinstance(emitter, Emitter):
            break
