import abc
import logging
import typing
from abc import ABC
from types import TracebackType
from typing import Protocol, Mapping, Any, TypeVar, Union, Tuple, Type, Iterable

from typing_extensions import TypeAlias, Literal

__all__ = (
    "SupportsStr",
    "MsgArgs",
    "Extra",
    "ExcInfo",
    "LogCtx",
    "LogMethod",
    "LeveledLogMethod",
    "LoggerLike",
    "LoggerLikeT",
    "LoggerLikeImpl",
    "DefaultRecordField",
    "default_record_fields",
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


class LogCtx(ABC):
    @property
    @abc.abstractmethod
    def prefix(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def args(self) -> MsgArgs:
        pass

    @property
    @abc.abstractmethod
    def extra(self) -> Extra:
        pass

    @property
    @abc.abstractmethod
    def frozen(self) -> bool:
        pass

    @abc.abstractmethod
    def __iter__(self) -> Iterable[Union[str, MsgArgs, Extra]]:
        pass


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


DefaultRecordField: TypeAlias = Literal[
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "msg",
    "message",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
]

default_record_fields = tuple(typing.get_args(DefaultRecordField))
