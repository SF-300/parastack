import abc
import logging
import typing
from abc import ABC
from types import TracebackType
from typing import Protocol, Mapping, Any, TypeVar, Union, Tuple, Type

from typing_extensions import TypeAlias

__all__ = (
    "SupportsStr",
    "MsgArgs",
    "Extra",
    "ExcInfo",
    "LogMethod",
    "LeveledLogMethod",
    "LoggerLike",
    "LoggerLikeT",
    "LoggerLikeImpl",
)


class SupportsStr(Protocol):
    def __str__(self) -> str:
        ...


MsgArgs: TypeAlias = Mapping[str, SupportsStr]
Extra: TypeAlias = Mapping[str, Any]
ExcInfo: TypeAlias = Union[
    None,
    BaseException,
    Tuple[None, None, None],
    Tuple[Type[BaseException], BaseException, TracebackType],
]


class LogMethod(Protocol):
    def __call__(
        self,
        level: int,
        msg: str,
        args: MsgArgs = ...,
        exc_info: ExcInfo = ...,
        stack_info: bool = ...,
        stacklevel: int = ...,
        extra: Extra = ...,
    ) -> None:
        ...


class LeveledLogMethod(Protocol):
    def __call__(
        self,
        msg: str,
        args: MsgArgs = ...,
        exc_info: ExcInfo = ...,
        stack_info: bool = ...,
        stacklevel: int = ...,
        extra: Extra = ...,
    ) -> None:
        ...


@typing.runtime_checkable
class LoggerLike(Protocol):
    log: LogMethod
    debug: LeveledLogMethod
    info: LeveledLogMethod
    warning: LeveledLogMethod
    error: LeveledLogMethod
    critical: LeveledLogMethod

    # noinspection PyPep8Naming
    def isEnabledFor(self, level: int) -> bool:
        ...


LoggerLikeT = TypeVar("LoggerLikeT", bound=LoggerLike)


def _create_leveled_log_method(level: int) -> LeveledLogMethod:
    def leveled_proxy(self, *args, stacklevel: int = 1, **kwargs):
        return self.log(level, *args, stacklevel=stacklevel + 1, **kwargs)

    return typing.cast(LeveledLogMethod, leveled_proxy)


class LoggerLikeImpl(ABC, LoggerLike):
    log = typing.cast(LogMethod, abc.abstractmethod(lambda *args, **kwargs: None))
    debug = _create_leveled_log_method(logging.DEBUG)
    info = _create_leveled_log_method(logging.INFO)
    warning = _create_leveled_log_method(logging.WARNING)
    error = _create_leveled_log_method(logging.ERROR)
    critical = _create_leveled_log_method(logging.CRITICAL)

    # noinspection PyPep8Naming
    @abc.abstractmethod
    def isEnabledFor(self, level: int) -> bool:
        pass
