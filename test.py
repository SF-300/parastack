import asyncio
import contextlib
import itertools
import logging
from contextlib import AsyncExitStack
from typing import AsyncContextManager

import parastack
from parastack import Dispatcher, Monitor
from parastack.event import MonitorMethodCalled


root_logger = logging.getLogger()


class _ServerMonitor(Monitor):
    pass


@contextlib.asynccontextmanager
async def _server_running(server_monitor: _ServerMonitor) -> AsyncContextManager[None]:
    async with AsyncExitStack() as deffer:
        await deffer.enter_async_context(_api_app_running(
            deffer.enter_context(_ApiAppMonitor(server_monitor))
        ))
        yield


class _ApiAppMonitor(Monitor):
    pass


@contextlib.asynccontextmanager
async def _api_app_running(api_app_monitor: _ApiAppMonitor) -> AsyncContextManager[None]:
    with _ResyncTaskMonitor(api_app_monitor) as resync_task_monitor:
        try:
            task = asyncio.create_task(_resync_task(resync_task_monitor))
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


class _ResyncTaskMonitor(Monitor):
    def test(self, msg: str = "Omg! It works!", *, stacklevel: int = 1):
        root_logger.info(msg, stacklevel=stacklevel, stack_info=True)


async def _resync_task(resync_task_monitor: _ResyncTaskMonitor):
    while True:
        resync_task_monitor.test()
        await asyncio.sleep(1)
        # return "Victory!"


async def test_basic():
    def build_dispatcher():

        def resync_task():
            try:
                for i in itertools.count():
                    yield from parastack.wait(lambda e: e == _ResyncTaskMonitor.test)
                    root_logger.info(f"test {i} received", stacklevel=parastack.stacklevel, stack_info=True)
            except parastack.MonitorExited:
                root_logger.debug("Exited received", stacklevel=parastack.stacklevel, stack_info=True)

        def root(event):
            # if event == _ResyncTaskMonitor.__enter__:
            #     dispatcher.spawn(event, resync_task())
            if isinstance(event, MonitorMethodCalled):
                event(stacklevel=parastack.stacklevel)
            # logging.debug(event, stacklevel=parastack.stacklevel)

        return (dispatcher := Dispatcher(root))

    async with AsyncExitStack() as deffer:
        await deffer.enter_async_context(_server_running(
            deffer.enter_context(_ServerMonitor(build_dispatcher()))
        ))
        await asyncio.sleep(5)
