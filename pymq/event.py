"""Events, hooks, and observer protocol for pymq.

Corresponds to gomq/event.go — Operation, EventStatus, Event, Hooks, Observer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import Any, Protocol, runtime_checkable


class Operation(str, Enum):
    ENQUEUE = "enqueue"
    CLAIM = "claim"
    ACK = "ack"
    NACK = "nack"
    RETRY = "retry"
    DLQ = "dlq"
    EXTEND = "extend"
    HANDLER = "handler"
    REAP = "reap"
    ADMIN = "admin"


class EventStatus(str, Enum):
    OK = "ok"
    ERROR = "error"
    UNEXPECTED = "unexpected"
    HANDLER_ERROR = "handler_error"
    RETRY = "retry"
    DLQ = "dlq"
    REAPED = "reaped"
    DUPLICATE = "duplicate"
    DONE = "done"
    ALREADY_DONE = "already_done"
    LOST_OWNERSHIP = "lost_ownership"
    REQUEUE_DLQ = "requeue_dlq"
    DELETE_DLQ = "delete_dlq"
    DELETE_DONE = "delete_done"
    PRUNE_DONE = "prune_done"
    OBSERVER_PANIC = "observer_panic"
    HOOK_PANIC = "hook_panic"


@dataclass
class Event:
    """An event emitted during queue operations.

    Corresponds to gomq/event.go Event struct.
    """

    operation: Operation
    status: EventStatus
    queue: str = ""
    message_id: str = ""
    attempt: int = 0
    duration: timedelta = field(default_factory=timedelta)
    end_to_end_duration: timedelta | None = None
    error: Exception | None = None


@dataclass
class Hooks:
    """Lifecycle hooks for queue events.

    Corresponds to gomq/event.go Hooks struct.
    Each hook receives an Event. Hooks run after the observer.
    Panic in a hook is caught and forwarded to on_error (if set).
    """

    on_claim: Any = None  # Callable[[Event], None] | None
    on_ack: Any = None
    on_retry: Any = None
    on_dlq: Any = None
    on_error: Any = None


@runtime_checkable
class Observer(Protocol):
    """Protocol for observing queue events.

    Corresponds to gomq/event.go Observer interface.
    """

    def observe(self, event: Event) -> None: ...
