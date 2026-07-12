"""Error types and sentinel exceptions for pymq.

Corresponds to gomq/errors.go — ErrorCode maps to Go's Code type,
GomqError maps to Go's Error struct, sentinels map to Go's Err* vars.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class ErrorCode(StrEnum):
    DUPLICATE = "duplicate"
    ALREADY_DONE = "already_done"
    LOST_OWNERSHIP = "lost_ownership"
    DLQ = "dlq"
    INVALID_CONFIG = "invalid_config"
    REDIS = "redis"
    UNEXPECTED = "unexpected"


class ErrorStatus(StrEnum):
    DUPLICATE = "duplicate"
    DONE = "done"
    ALREADY_DONE = "already_done"
    LOST_OWNERSHIP = "lost_ownership"
    DLQ = "dlq"
    MISSING = "missing"
    NIL_DELIVERY = "nil_delivery"
    UNEXPECTED = "unexpected"


class GomqError(Exception):
    """Primary error type for pymq operations.

    Corresponds to gomq/errors.go Error struct.
    """

    def __init__(
        self,
        code: ErrorCode,
        op: str = "",
        queue: str = "",
        message_id: str = "",
        status: ErrorStatus | str = "",
        cause: Exception | None = None,
        message: str = "",
    ) -> None:
        self.code = code
        self.op = op
        self.queue = queue
        self.message_id = message_id
        self.status = status
        self.cause = cause
        self.message = message
        super().__init__(self._format())

    def _format(self) -> str:
        msg = f"pymq: {self.code.value}"
        if self.op:
            msg += f" op={self.op}"
        if self.queue:
            msg += f" queue={self.queue}"
        if self.message_id:
            msg += f" message_id={self.message_id}"
        if self.status:
            s = self.status.value if isinstance(self.status, ErrorStatus) else self.status
            msg += f" status={s}"
        if self.message:
            msg += f": {self.message}"
        elif self.cause:
            msg += f": {self.cause}"
        return msg

    def __reduce__(self) -> tuple[Any, ...]:
        return (
            self.__class__,
            (self.code, self.op, self.queue, self.message_id, self.status, self.cause),
        )


class PermanentError(GomqError):
    """Wraps an error to signal immediate DLQ without retry.

    Corresponds to gomq/errors.go PermanentError.
    """

    def __init__(
        self,
        cause: Exception | None = None,
        *,
        op: str = "",
        queue: str = "",
        message_id: str = "",
        status: ErrorStatus | str = "",
        message: str = "",
    ) -> None:
        super().__init__(
            code=ErrorCode.UNEXPECTED,
            op=op,
            queue=queue,
            message_id=message_id,
            status=status,
            cause=cause,
            message=message,
        )

    def _format(self) -> str:
        return f"permanent: {self.cause}" if self.cause else "permanent"


def is_permanent(err: Exception | None) -> bool:
    """Check if an error is permanent (should go to DLQ immediately)."""
    return isinstance(err, PermanentError)


# Sentinel errors — correspond to gomq/errors.go Err* vars
class NoMessageError(GomqError):
    """No message available in the queue."""

    def __init__(self) -> None:
        super().__init__(code=ErrorCode.UNEXPECTED, status=ErrorStatus.MISSING)


class DuplicateError(GomqError):
    """Message ID is a duplicate of an active message."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(code=ErrorCode.DUPLICATE, **kwargs)


class AlreadyDoneError(GomqError):
    """Message was already processed (dedup)."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(code=ErrorCode.ALREADY_DONE, **kwargs)


class LostOwnershipError(GomqError):
    """Fencing token no longer valid — another consumer claimed this message."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(code=ErrorCode.LOST_OWNERSHIP, **kwargs)


class InvalidConfigError(GomqError):
    """Invalid configuration."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(code=ErrorCode.INVALID_CONFIG, **kwargs)


class DLQError(GomqError):
    """Message moved to dead-letter queue."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(code=ErrorCode.DLQ, **kwargs)
