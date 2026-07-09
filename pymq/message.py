"""Message and Delivery types with generic payload support.

Corresponds to gomq/message.go — Message struct and Delivery struct.
Uses Pydantic Generic[T] for typed payloads.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass
class Message(Generic[T]):
    """A message to enqueue.

    Corresponds to gomq/message.go Message struct.

    The payload can be any type T. If T is a Pydantic BaseModel, it will be
    serialized to JSON bytes via TypeAdapter before storage. If T is bytes,
    it is stored directly.
    """

    id: str = ""
    payload: Any = b""
    headers: dict[str, str] = field(default_factory=dict)
    delay: timedelta = field(default_factory=timedelta)


@dataclass
class Delivery(Generic[T]):
    """A received message with ack/nack/extend capabilities.

    Corresponds to gomq/message.go Delivery struct.

    The payload is deserialized to type T based on the handler's type annotation.
    The queue reference is used internally for ack/nack/extend operations.
    """

    _queue: Any = field(default=None, repr=False)
    _enqueued_at_ms: int = field(default=0, repr=False)

    id: str = ""
    payload: Any = None
    headers: dict[str, str] = field(default_factory=dict)
    token: str = ""
    attempts: int = 0
    lease_until_ms: int = 0

    async def ack(self) -> None:
        """Acknowledge successful processing.

        Raises LostOwnershipError if the fencing token is no longer valid.
        """
        if self._queue is None:
            from pymq.errors import LostOwnershipError

            raise LostOwnershipError(status="nil_delivery")
        await self._queue.ack(self.id, self.token, self._enqueued_at_ms)

    async def nack(
        self,
        *,
        permanent: bool = False,
        backoff: timedelta | None = None,
        error: Exception | None = None,
    ) -> None:
        """Reject the message for retry or DLQ.

        Args:
            permanent: If True, skip retry and go directly to DLQ.
            backoff: Custom backoff duration. If None, uses queue default.
            error: The error that caused the rejection (stored in Redis).
        """
        if self._queue is None:
            from pymq.errors import LostOwnershipError

            raise LostOwnershipError(status="nil_delivery")
        await self._queue.nack(
            self.id,
            self.token,
            self.attempts,
            permanent=permanent,
            backoff=backoff,
            error=error,
        )

    async def extend(self, timeout: timedelta) -> None:
        """Extend the visibility timeout (lease) for this message.

        Raises LostOwnershipError if the fencing token is no longer valid.
        """
        if self._queue is None:
            from pymq.errors import LostOwnershipError

            raise LostOwnershipError(status="nil_delivery")
        await self._queue.extend(self.id, self.token, timeout)
