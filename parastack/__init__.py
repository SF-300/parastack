"""Send events from execution stack to parallel mirroring stack (mostly intended for monitoring)."""

from parastack.dispatcher import Dispatcher
from parastack.emitter import Emitter, Forked, Joined, void_emitter
from parastack.utils import wait, wait_through, NamespaceMeta

__version__ = "0.2.0"

__all__ = (
    "Dispatcher",
    "Emitter", "Forked", "Joined", "void_emitter",
    "wait", "wait_through", "NamespaceMeta",
    "__version__",
)
