"""Event observer and hook behavior parity tests."""

from pymq.event import Event, EventStatus, Hooks, ObserverFunc, Operation
from pymq.queue import Queue


async def test_hooks_and_observer_receive_equivalent_events(redis_client):
    hooks = []
    observed = []

    class Recorder:
        def observe(self, event: Event) -> None:
            observed.append(f"{event.operation.value}:{event.status.value}")

    queue = Queue(
        redis_client,
        queue_name="events",
        observer=Recorder(),
        hooks=Hooks(
            on_claim=lambda _event: hooks.append("claim"),
            on_ack=lambda _event: hooks.append("ack"),
            on_retry=lambda _event: hooks.append("retry"),
            on_dlq=lambda _event: hooks.append("dlq"),
            on_error=lambda _event: hooks.append("error"),
        ),
    )

    queue._emit(Event(operation=Operation.CLAIM, status=EventStatus.OK))
    queue._emit(Event(operation=Operation.ACK, status=EventStatus.OK))
    queue._emit(
        Event(
            operation=Operation.ACK,
            status=EventStatus.LOST_OWNERSHIP,
            error=RuntimeError("lost"),
        )
    )
    queue._emit(
        Event(
            operation=Operation.RETRY,
            status=EventStatus.RETRY,
            error=RuntimeError("handler"),
        )
    )
    queue._emit(
        Event(
            operation=Operation.DLQ,
            status=EventStatus.DLQ,
            error=RuntimeError("handler"),
        )
    )

    assert hooks == ["claim", "ack", "error", "error", "error"]
    assert observed == [
        "claim:ok",
        "ack:ok",
        "ack:lost_ownership",
        "retry:retry",
        "dlq:dlq",
    ]


async def test_observer_and_hook_failures_are_reported(redis_client):
    errors = []

    class BrokenObserver:
        def observe(self, _event: Event) -> None:
            raise RuntimeError("observer")

    def broken_hook(_event: Event) -> None:
        raise RuntimeError("hook")

    queue = Queue(
        redis_client,
        queue_name="event-errors",
        observer=BrokenObserver(),
        hooks=Hooks(on_claim=broken_hook, on_error=errors.append),
    )
    queue._emit(Event(operation=Operation.CLAIM, status=EventStatus.OK))

    assert [event.status for event in errors] == [
        EventStatus.OBSERVER_PANIC,
        EventStatus.HOOK_PANIC,
    ]


def test_observer_func_adapts_callback():
    events = []
    observer = ObserverFunc(events.append)
    event = Event(operation=Operation.ENQUEUE, status=EventStatus.OK)
    observer.observe(event)
    assert events == [event]
