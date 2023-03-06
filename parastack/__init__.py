"""Send events from execution stack and aggregate them in a parallel stack."""

from parastack.dispatcher import Dispatcher
from parastack.emitter import Emitter, Forked, Joined, void_emitter
from parastack.utils import WaitCancelled, wait, wait_through, NamespaceMeta, emits

__version__ = "0.3.0"

__all__ = (
    "Dispatcher",
    "Emitter", "Forked", "Joined", "void_emitter",
    "WaitCancelled", "wait", "wait_through", "NamespaceMeta", "emits",
    "__version__",
)
