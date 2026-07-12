"""Tests for optional Prometheus and OpenTelemetry adapters."""

from datetime import timedelta

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from pymq.event import Event, EventStatus, Operation
from pymq.message import Message
from pymq.observability.otel import Observer as OTelObserver
from pymq.observability.prometheus import (
    Observer as PrometheusObserver,
)
from pymq.observability.prometheus import (
    QueueCollector,
    QueueCollectorOptions,
)
from pymq.queue import Queue


def test_prometheus_observer_records_equivalent_metrics():
    registry = CollectorRegistry()
    observer = PrometheusObserver(registry)
    observer.observe(
        Event(
            queue="jobs",
            operation=Operation.ACK,
            status=EventStatus.OK,
            duration=timedelta(milliseconds=5),
            end_to_end_duration=timedelta(milliseconds=50),
        )
    )

    metrics = generate_latest(registry).decode()
    assert 'gomq_events_total{operation="ack",queue="jobs",status="ok"} 1.0' in metrics
    assert "gomq_operation_duration_seconds_count" in metrics
    assert "gomq_message_end_to_end_duration_seconds_count" in metrics


async def test_prometheus_queue_collector(queue: Queue):
    await queue.enqueue(Message(payload=b"ready"), id="ready")
    registry = CollectorRegistry()
    collector = QueueCollector(
        QueueCollectorOptions(queues=[queue], interval=timedelta(0), registry=registry)
    )

    await collector.collect_once()
    metrics = generate_latest(registry).decode()
    expected = f'gomq_queue_depth{{queue="{queue.queue_name}",state="ready"}} 1.0'
    assert expected in metrics


def test_prometheus_queue_collector_validation():
    with pytest.raises(ValueError):
        QueueCollector(QueueCollectorOptions())


async def test_prometheus_queue_collector_propagates_and_counts_errors():
    class BrokenQueue:
        queue_name = "broken"

        async def stats(self):
            raise RuntimeError("stats failed")

    registry = CollectorRegistry()
    collector = QueueCollector(
        QueueCollectorOptions(queues=[BrokenQueue()], registry=registry)
    )
    with pytest.raises(RuntimeError, match="stats failed"):
        await collector.collect_once()
    metrics = generate_latest(registry).decode()
    assert 'gomq_queue_stats_collect_errors_total{queue="broken"} 1.0' in metrics


class _Instrument:
    def __init__(self) -> None:
        self.calls = []

    def add(self, value, *, attributes) -> None:
        self.calls.append((value, attributes))

    def record(self, value, *, attributes) -> None:
        self.calls.append((value, attributes))


class _Meter:
    def __init__(self) -> None:
        self.instruments = {}

    def create_counter(self, name):
        return self.instruments.setdefault(name, _Instrument())

    def create_histogram(self, name):
        return self.instruments.setdefault(name, _Instrument())


def test_otel_observer_records_instruments_and_attributes():
    meter = _Meter()
    observer = OTelObserver(meter)
    observer.observe(
        Event(
            queue="jobs",
            operation=Operation.NACK,
            status=EventStatus.ERROR,
            duration=timedelta(milliseconds=10),
            error=RuntimeError("failed"),
        )
    )

    expected = {"queue": "jobs", "operation": "nack", "status": "error"}
    assert meter.instruments["gomq.events"].calls == [(1, expected)]
    assert meter.instruments["gomq.errors"].calls == [(1, expected)]
    assert meter.instruments["gomq.operation.duration"].calls == [(0.01, expected)]
