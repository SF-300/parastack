import asyncio
import contextlib
import inspect
import sys
import typing
from typing import ParamSpec, TypeVar, Callable, ContextManager, Protocol, Literal, TypeAlias, Type

from typing_extensions import Self

__all__ = "Emitter", "Forked", "Terminated", "Joined", "void_emitter"

_P = ParamSpec("_P")
_R = TypeVar("_R")


@typing.runtime_checkable
class Forked(Protocol):
    child: "Emitter"

    # def __str__(self):
    #     pass


class Terminated(Exception):
    pass


class Joined(Terminated):
    @property
    def cause(self) -> Exception | None:
        return self.__cause__


class _Dispatcher(Protocol):
    def __call__(self, emitter: "Emitter", event: object) -> None:
        ...


class _ForkedEventFactory(Protocol):
    def __call__(self, child: "Emitter") -> Forked:
        ...


_CreateForkEvent: TypeAlias = _ForkedEventFactory | Type[Forked]


class Emitter:
    def __init__(self, dispatcher: _Dispatcher) -> None:
        self._dispatcher = dispatcher

    def send(self, event: object) -> None:
        self._dispatcher(self, event)

    def forked(self, create_forked_event: _CreateForkEvent = Forked) -> "_ForkedEmitter":
        return _ForkedEmitter(self._dispatcher, self, create_forked_event)


class _ForkedEmitter(Emitter):
    def __init__(self, dispatcher: _Dispatcher, parent: Emitter, create_forked_event: _CreateForkEvent) -> None:
        super().__init__(dispatcher)
        self._parent = parent

        dispatcher(parent, create_forked_event(self))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        event = Joined()
        if exc_val is not None:
            event.__cause__ = exc_val
        self._dispatcher(self, event)

    def __call__(self, wrapped: Callable[[Emitter], _R]) -> _R:
        try:
            result = wrapped(self)
        except Exception:
            self.__exit__(*sys.exc_info())
            raise
        # NOTE(zeronineseven): asyncio special-casing.
        if inspect.iscoroutine(result):
            async def finalizer() -> _R:
                exc_type, exc_val, exc_tb = None, None, None
                try:
                    inner_result = await result
                except Exception:
                    exc_type, exc_val, exc_tb = sys.exc_info()
                    raise
                else:
                    return inner_result
                finally:
                    self.__exit__(exc_type, exc_val, exc_tb)
            return finalizer()
        elif asyncio.isfuture(result):
            def on_done(f: asyncio.Future):
                exc_type, exc_val, exc_tb = None, None, None
                try:
                    e = f.exception()
                except asyncio.CancelledError:
                    pass
                else:
                    exc_type, exc_val, exc_tb = type(e), e, e.__traceback__
                self.__exit__(exc_type, exc_val, exc_tb)
            result.add_done_callback(on_done)
            return result
        else:
            self.__exit__(None, None, None)
            return result


@typing.final
class _VoidEmitter(Emitter):
    def send(self, event: object) -> None:
        pass

    @contextlib.contextmanager
    def forked(self, create_forked_event: Callable[["Emitter"], "Forked"] = ...) -> ContextManager[Self]:
        yield self

    def __bool__(self) -> Literal[False]:
        return False


void_emitter = _VoidEmitter(lambda *args, **kwargs: None)
