import typing
import warnings
import weakref
import inspect
from typing import Union, Iterable, TypeAlias, Callable, Dict, Generator, NewType, Any
from unittest.mock import MagicMock

from parastack._logging import LoggerLike
from parastack.emitter import Emitter, Forked, Joined, Terminated

__all__ = "HandlerDone", "Dispatcher"

_EmitterHandler: TypeAlias = Callable[[Emitter, object], None]
_EmitterId = NewType("_EmitterId", int)


@typing.final
class HandlerDone(Terminated):
    pass


@typing.final
class EmitterDeallocated(Terminated):
    pass


_GenEmitterHandler: TypeAlias = Generator[None, object, None]
_FuncEmitterHandler: TypeAlias = Callable[[object], None]
_AnyEmitterHandler: TypeAlias = Union[_GenEmitterHandler, _FuncEmitterHandler]


class _NormalizedEmitterHandler:
    def __init__(self, handler: _AnyEmitterHandler, logger: LoggerLike) -> None:
        if inspect.isgeneratorfunction(handler):
            warnings.warn(
                "Avoid passing generator functions as handlers - "
                "instantiate the generators and pass them directly instead."
            )
            handler = handler()
            assert inspect.isgenerator(handler)
        if inspect.isgenerator(handler):
            if inspect.getgeneratorstate(handler) == inspect.GEN_CREATED:
                next(handler)

            def send(event: object) -> None:
                try:
                    handler.send(event)
                except HandlerDone as e:
                    raise e
                except Exception as e:
                    raise HandlerDone() from e

            def throw(exc: Exception) -> None:
                handler.throw(type(exc), exc, exc.__traceback__)
        else:
            def send(event: object) -> None:
                try:
                    handler(event)
                except Exception as e:
                    if not isinstance(e, HandlerDone):
                        # NOTE(zeronineseven): Swallow all exceptions raised by stateless function handler unless
                        #                      it's HandlerDone exception which explicitly signals that handler
                        #                      should not be called anymore.
                        logger.debug("Ignoring exception raised by function handler for '%(eid)s' emitter.", exc_info=e)
                        return
                    raise e

            def throw(e: Exception) -> None:
                raise e

        self.send, self.throw = send, throw


class Dispatcher:
    def __init__(self, root_handler: Callable[[Any], Any], logger: LoggerLike = MagicMock()) -> None:
        def root_handler_wrapper(_, event: object) -> None:
            try:
                root_handler(event)
            except Exception as e:
                logger.debug(f"Root handler failed to handle event '{event}'!", exc_info=e)

        self.__root_handler = root_handler_wrapper
        self.__eid_to_handler: Dict[_EmitterId, _EmitterHandler] = dict()
        self.__logger = logger
        self.__current_event = None

    @property
    def stacklevel(self) -> int:
        assert self.__current_event is not None
        if isinstance(self.__current_event, (Forked, Joined)):
            return 9
        return 6

    def fork(self, forked: Forked, handler: _AnyEmitterHandler) -> None:
        # NOTE(zeronineseven): There are 3 ways which can cause handler to be discarded:
        #                      1. Handler has received 'Joined' event (expected scenario).
        #                      2. Emitter got deallocated while handler is still alive.
        #                      3. Generator-based handler has prematurely raised some exception
        #                         when Emitter is still alive.
        eid = typing.cast(_EmitterId, id(forked.child))
        assert eid not in self.__eid_to_handler

        # NOTE(zeronineseven): Be careful not to capture 'forked' in any nested closures!
        def cleanup():
            finalizer.detach()
            del self.__eid_to_handler[eid]

        handler = _NormalizedEmitterHandler(handler, self.__logger)

        def handler_wrapper(emitter: Emitter, event: object):
            try:
                handler.send(event)
                if isinstance(event, Joined) and id(emitter) == eid:
                    # NOTE(zeronineseven): If we're here, it means that it was this handler, which has been joined, but
                    #                      for some reason it decided to ignore own termination.
                    try:
                        handler.throw(event)
                    except Exception as e:
                        if e != event:
                            self.__logger.warning(
                                f"Handler for emitter with id '{eid}' raised an "
                                f"'{type(e)}' exception while handling own termination event.",
                                exc_info=e
                            )
                        raise HandlerDone() from e
            except HandlerDone:
                cleanup()

        def on_deallocated():
            self.__logger.debug(f"Emitter with id '{eid}' got deallocated.")
            try:
                handler.throw(EmitterDeallocated())
            except EmitterDeallocated:
                pass
            except Exception as e:
                self.__logger.warning(
                    f"Handler for emitter with id '{eid}' raised an "
                    f"'{type(e)}' exception while handling emitter deallocation.",
                    exc_info=e
                )
            cleanup()

        finalizer = weakref.finalize(forked.child, on_deallocated)

        self.__eid_to_handler[eid] = handler_wrapper

    def __call__(self, emitter: Emitter, event: object) -> None:
        def find_handler() -> _EmitterHandler:
            for eid in _emitters_ids_chain_iter(emitter):
                try:
                    return self.__eid_to_handler[eid]
                except KeyError:
                    continue
            return self.__root_handler

        self.__current_event = event
        try:
            find_handler()(emitter, event)
        finally:
            self.__current_event = None


def _emitters_ids_chain_iter(emitter: Emitter) -> Iterable[_EmitterId]:
    assert isinstance(emitter, Emitter)
    while True:
        yield typing.cast(_EmitterId, id(emitter))
        emitter = getattr(emitter, "_parent", None)
        if not isinstance(emitter, Emitter):
            break
