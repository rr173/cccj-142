"""Exception hierarchy for the iterflow runtime."""


class IterflowError(Exception):
    """Base class for all iterflow errors."""


class GraphError(IterflowError):
    """Static graph validation failure (e.g. a cycle with no feedback edge)."""


class SourceStateError(IterflowError):
    """Illegal source-side state transition (duplicate close, send after close, ...)."""


class UndeclaredEpochError(SourceStateError):
    """Epoch was not declared before the run started."""


class SourceClosedError(SourceStateError):
    """Source already closed this epoch (or was never asked to close it)."""


class DuplicateMessageError(IterflowError):
    """Caller-provided message id was already used on this source."""


class RunStoppedError(IterflowError):
    """Raised inside callers/workers when the run is no longer RUNNING."""


class LimitExceededError(IterflowError):
    """A configured run limit (max_round / max_derived) was exceeded."""

    def __init__(self, kind, detail):
        super().__init__(f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail


class RunFailedError(IterflowError):
    """The run terminated in FAILED state; delivered to released waiters."""

    def __init__(self, kind, detail):
        super().__init__(f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail


class RunCancelledError(IterflowError):
    """The run was cancelled by the caller; delivered to released waiters."""
