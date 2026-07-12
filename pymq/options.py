"""Queue configuration.

Corresponds to gomq/options.go — Options struct with defaults and validation.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum

from pymq.errors import InvalidConfigError
from pymq.event import Hooks, Observer
from pymq.middleware import ConsumerMiddleware, ProducerMiddleware


def default_backoff(attempt: int) -> timedelta:
    """Default retry backoff: attempt * 1s, capped at 30s.

    Mirrors gomq/options.go default RetryBackoff.
    """
    a = max(attempt, 1)
    d = timedelta(seconds=a)
    cap = timedelta(seconds=30)
    return min(d, cap)


class AckRetentionPolicy(StrEnum):
    """Controls whether completed job data remains available after ACK."""

    RETAIN_ACKED = "retain_acked"
    DELETE_ACKED = "delete_acked"


@dataclass
class Options:
    """Queue configuration with validated defaults.

    Corresponds to gomq/options.go Options struct.
    All fields have sensible defaults matching the Go implementation.
    """

    queue_name: str
    visibility_timeout: timedelta = field(default=timedelta(seconds=30))
    poll_timeout: timedelta = field(default=timedelta(seconds=1))
    poll_interval: timedelta = field(default=timedelta(milliseconds=50))
    heartbeat_interval: timedelta | None = None
    dedupe_ttl: timedelta = field(default=timedelta(hours=24))
    done_retention_ttl: timedelta = field(default_factory=timedelta)
    max_attempts: int = 3
    consumer_id: str = "pymq-consumer"
    ack_retention_policy: AckRetentionPolicy = AckRetentionPolicy.RETAIN_ACKED
    retry_backoff: Callable[[int], timedelta] | None = None

    # Observability
    hooks: Hooks | None = None
    observer: Observer | None = None

    # Middlewares
    consumer_middlewares: list[ConsumerMiddleware] = field(default_factory=lambda: [])
    producer_middlewares: list[ProducerMiddleware] = field(default_factory=lambda: [])

    def __post_init__(self) -> None:
        if not self.queue_name:
            raise InvalidConfigError(op="options", message="queue_name is required")
        if self.visibility_timeout == timedelta(0):
            self.visibility_timeout = timedelta(seconds=30)
        if self.poll_timeout == timedelta(0):
            self.poll_timeout = timedelta(seconds=1)
        if self.poll_interval == timedelta(0):
            self.poll_interval = timedelta(milliseconds=50)
        if self.heartbeat_interval is None or self.heartbeat_interval == timedelta(0):
            self.heartbeat_interval = self.visibility_timeout / 3
        if self.dedupe_ttl == timedelta(0):
            self.dedupe_ttl = timedelta(hours=24)
        if self.max_attempts == 0:
            self.max_attempts = 3
        if not self.consumer_id:
            self.consumer_id = "pymq-consumer"
        if self.retry_backoff is None:
            self.retry_backoff = default_backoff
        self._validate()

    def _validate(self) -> None:
        errors: list[str] = []
        if self.visibility_timeout <= timedelta(0):
            errors.append("visibility_timeout must be positive")
        if self.poll_timeout < timedelta(0):
            errors.append("poll_timeout cannot be negative")
        if self.poll_interval <= timedelta(0):
            errors.append("poll_interval must be positive")
        if self.heartbeat_interval is not None and self.heartbeat_interval <= timedelta(0):
            errors.append("heartbeat_interval must be positive")
        if self.max_attempts <= 0:
            errors.append("max_attempts must be positive")
        if self.dedupe_ttl < timedelta(0):
            errors.append("dedupe_ttl cannot be negative")
        if self.done_retention_ttl < timedelta(0):
            errors.append("done_retention_ttl cannot be negative")
        if self.ack_retention_policy not in (
            AckRetentionPolicy.RETAIN_ACKED,
            AckRetentionPolicy.DELETE_ACKED,
        ):
            errors.append("ack_retention_policy must be RETAIN_ACKED or DELETE_ACKED")
        if (
            self.heartbeat_interval is not None
            and self.visibility_timeout > timedelta(0)
            and self.heartbeat_interval >= self.visibility_timeout
        ):
            errors.append("heartbeat_interval must be smaller than visibility_timeout")
        if self.poll_interval > self.poll_timeout > timedelta(0):
            errors.append("poll_interval must be <= poll_timeout")
        if self.retry_backoff is not None:
            for attempt in (1, self.max_attempts):
                if self.retry_backoff(attempt) < timedelta(0):
                    errors.append("retry_backoff must not return negative duration")
                    break
        if errors:
            raise InvalidConfigError(
                op="options",
                message="; ".join(errors),
            )


@dataclass
class EnqueueOptions:
    """Options for a single enqueue call.

    Corresponds to gomq/options.go enqueueConfig + EnqueueOption.
    """

    id: str = ""
    delay: timedelta = field(default_factory=timedelta)


@dataclass
class NackOptions:
    """Options for a single nack call.

    Corresponds to gomq/options.go nackConfig + NackOption.
    """

    permanent: bool = False
    backoff: timedelta | None = None
    error: Exception | None = None


@dataclass
class ListOptions:
    """Options for DLQ listing.

    Corresponds to gomq/admin.go ListOptions.
    """

    offset: int = 0
    count: int = 100


@dataclass
class RequeueOptions:
    """Options for requeuing from DLQ.

    Corresponds to gomq/admin.go requeueConfig.
    """

    delay: timedelta = field(default_factory=timedelta)
    reset_attempts: bool = False
