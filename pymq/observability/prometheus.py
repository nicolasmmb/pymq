"""Prometheus observer and queue statistics collector."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram

from pymq.errors import GomqError
from pymq.event import Event, EventStatus, Operation

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pymq.queue import Queue


class Observer:
    """Record queue events using the metric names and labels exposed by gomq."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        registry = registry or REGISTRY
        self.events = Counter(
            "gomq_events_total",
            "Total gomq events by queue, operation and status.",
            ("queue", "operation", "status"),
            registry=registry,
        )
        self.errors = Counter(
            "gomq_errors_total",
            "Total gomq errors by queue, operation, status and code.",
            ("queue", "operation", "status", "code"),
            registry=registry,
        )
        self.latency = Histogram(
            "gomq_operation_duration_seconds",
            "gomq operation latency by queue, operation and status.",
            ("queue", "operation", "status"),
            registry=registry,
        )
        self.end_to_end = Histogram(
            "gomq_message_end_to_end_duration_seconds",
            "gomq message latency from enqueue to ack by queue and status.",
            ("queue", "status"),
            registry=registry,
        )
        self.inflight = Gauge(
            "gomq_inflight_events",
            "Current observed in-flight claim/ack delta by queue.",
            ("queue",),
            registry=registry,
        )

    def observe(self, event: Event) -> None:
        queue = event.queue
        operation = event.operation.value
        status = event.status.value
        self.events.labels(queue, operation, status).inc()
        if event.duration > timedelta(0):
            self.latency.labels(queue, operation, status).observe(
                event.duration.total_seconds()
            )
        if event.end_to_end_duration is not None and event.end_to_end_duration > timedelta(0):
            self.end_to_end.labels(queue, status).observe(
                event.end_to_end_duration.total_seconds()
            )
        if event.error is not None:
            code = event.error.code.value if isinstance(event.error, GomqError) else "error"
            self.errors.labels(queue, operation, status, code).inc()

        if event.operation == Operation.CLAIM and event.error is None:
            self.inflight.labels(queue).inc()
        elif (
            event.operation == Operation.ACK
            and event.error is None
            and event.status == EventStatus.OK
        ) or (
            event.operation == Operation.RETRY
            and event.status in {EventStatus.RETRY, EventStatus.REAPED}
        ) or (
            event.operation == Operation.DLQ
            and event.status in {EventStatus.DLQ, EventStatus.REAPED}
        ):
            self.inflight.labels(queue).dec()


@dataclass
class QueueCollectorOptions:
    """Configuration for periodic queue statistics collection."""

    queues: Sequence[Queue | None] = field(default_factory=tuple)
    interval: timedelta = timedelta(seconds=5)
    timeout: timedelta = timedelta(seconds=1)
    registry: CollectorRegistry | None = None


class QueueCollector:
    """Poll Queue.stats and expose depth and oldest-message gauges."""

    def __init__(self, options: QueueCollectorOptions) -> None:
        if not options.queues:
            raise ValueError("pymq prometheus: at least one queue is required")
        interval = options.interval or timedelta(seconds=5)
        timeout = options.timeout or timedelta(seconds=1)
        if interval < timedelta(0):
            raise ValueError("pymq prometheus: interval cannot be negative")
        if timeout <= timedelta(0):
            raise ValueError("pymq prometheus: timeout must be positive")

        self.queues = options.queues
        self.interval = interval
        self.timeout = timeout
        registry = options.registry or REGISTRY
        self.depth = Gauge(
            "gomq_queue_depth",
            "Current gomq queue depth by queue and state.",
            ("queue", "state"),
            registry=registry,
        )
        self.oldest_age = Gauge(
            "gomq_queue_oldest_message_age_seconds",
            "Age of the oldest gomq message by queue and state.",
            ("queue", "state"),
            registry=registry,
        )
        self.collect_errors = Counter(
            "gomq_queue_stats_collect_errors_total",
            "Total gomq queue stats collection errors by queue.",
            ("queue",),
            registry=registry,
        )

    async def start(self) -> None:
        with contextlib.suppress(Exception):
            await self.collect_once()
        if self.interval <= timedelta(0):
            return
        while True:
            await asyncio.sleep(self.interval.total_seconds())
            with contextlib.suppress(Exception):
                await self.collect_once()

    async def collect_once(self) -> None:
        first_error: Exception | None = None
        for queue in self.queues:
            if queue is None:
                continue
            try:
                stats = await asyncio.wait_for(
                    queue.stats(), timeout=self.timeout.total_seconds()
                )
            except Exception as error:
                self.collect_errors.labels(queue.queue_name).inc()
                if first_error is None:
                    first_error = error
                continue

            for state, value in (
                ("ready", stats.ready),
                ("inflight", stats.inflight),
                ("dlq", stats.dlq),
                ("done", stats.done),
            ):
                self.depth.labels(queue.queue_name, state).set(value)
            self.oldest_age.labels(queue.queue_name, "ready").set(
                stats.oldest_ready_age.total_seconds()
            )
            self.oldest_age.labels(queue.queue_name, "dlq").set(
                stats.oldest_dlq_age.total_seconds()
            )
        if first_error is not None:
            raise first_error
