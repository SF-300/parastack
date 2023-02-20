import asyncio
import contextlib
import itertools
import logging
from typing import AsyncContextManager

import parastack
from dataclasses import dataclass


root_logger = logging.getLogger()


@dataclass(frozen=True)
class ResyncTaskStarted(parastack.Forked):
    pass


@dataclass
class ResyncHappened:
    pass


@contextlib.asynccontextmanager
async def _server_running(monitoring: parastack.Emitter) -> AsyncContextManager[None]:
    async with _api_app_running(monitoring):
        yield


@contextlib.asynccontextmanager
async def _api_app_running(monitoring: parastack.Emitter) -> AsyncContextManager[None]:
    task = monitoring.forked(ResyncTaskStarted)(lambda m: asyncio.create_task(_resync_task(m)))
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def _resync_task(monitoring: parastack.Emitter):
    while True:
        monitoring.send(ResyncHappened())
        await asyncio.sleep(1)
        # return "Victory!"


async def test_basic():
    def build_dispatcher():

        def resync_task():
            try:
                root_logger.info(f"Started", stacklevel=dsp.stacklevel, stack_info=True)
                for i in itertools.count():
                    yield from parastack.wait({ResyncHappened})
                    root_logger.info(f"test {i} received", stacklevel=dsp.stacklevel, stack_info=True)
                root_logger.debug("Exited received", stacklevel=dsp.stacklevel, stack_info=True)
            except Exception as e:
                root_logger.info(f"Finished", stacklevel=dsp.stacklevel, stack_info=True)

        def root(event):
            if isinstance(event, ResyncTaskStarted):
                dsp.fork(event, resync_task())

        return (dsp := parastack.Dispatcher(root))

    async with _server_running(parastack.Emitter(build_dispatcher())):
        await asyncio.sleep(5)
