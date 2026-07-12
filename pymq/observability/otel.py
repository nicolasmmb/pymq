"""OpenTelemetry metrics observer."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from pymq.event import Event


class Observer:
    """Record gomq-compatible counters and latency through an OTel meter."""

    def __init__(self, meter: Any) -> None:
        self.events = meter.create_counter("gomq.events")
        self.errors = meter.create_counter("gomq.errors")
        self.latency = meter.create_histogram("gomq.operation.duration")

    def observe(self, event: Event) -> None:
        attributes = {
            "queue": event.queue,
            "operation": event.operation.value,
            "status": event.status.value,
        }
        self.events.add(1, attributes=attributes)
        if event.duration > timedelta(0):
            self.latency.record(event.duration.total_seconds(), attributes=attributes)
        if event.error is not None:
            self.errors.add(1, attributes=attributes)

