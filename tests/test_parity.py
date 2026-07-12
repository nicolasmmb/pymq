"""Cross-runtime behavioral cases mirrored from gomq integration tests."""

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from pymq.errors import LostOwnershipError, NoMessageError
from pymq.event import Event, EventStatus, Operation
from pymq.lifecycle import consume_with_signals
from pymq.message import Message
from pymq.state import MessageState


async def test_concurrent_claim_returns_one_owner(queue):
    queue._options.poll_timeout = timedelta(milliseconds=20)
    await queue.enqueue(Message(payload=b"once"), id="single")

    results = await asyncio.gather(*(queue.receive() for _ in range(25)), return_exceptions=True)
    deliveries = [result for result in results if not isinstance(result, Exception)]
    errors = [result for result in results if isinstance(result, Exception)]

    assert len(deliveries) == 1
    assert all(isinstance(error, NoMessageError) for error in errors)
    await deliveries[0].ack()


async def test_lost_ownership_after_reap(queue):
    queue._options.visibility_timeout = timedelta(milliseconds=30)
    queue._options.retry_backoff = lambda _attempt: timedelta(0)
    await queue.enqueue(Message(payload=b"slow"), id="slow")
    first = await queue.receive()

    await asyncio.sleep(0.05)
    assert await queue.reap_expired(10) == 1
    second = await queue.receive()

    with pytest.raises(LostOwnershipError):
        await first.ack()
    await second.ack()


async def test_nack_moves_to_dlq_at_max_attempts(queue):
    queue._options.max_attempts = 1
    await queue.enqueue(Message(payload=b"bad"), id="bad")
    delivery = await queue.receive()
    await delivery.nack(error=RuntimeError("boom"))

    info = await queue.inspect("bad")
    assert info.state == MessageState.DLQ
    assert info.last_error == "boom"
    assert (await queue.stats()).dlq == 1


async def test_inspect_states_and_stats(queue):
    await queue.enqueue(
        Message(payload=b"stateful", headers={"kind": "state"}),
        id="stateful",
    )
    stats = await queue.stats()
    assert (stats.ready, stats.inflight, stats.dlq) == (1, 0, 0)

    ready = await queue.inspect("stateful")
    assert ready.state == MessageState.READY
    assert ready.headers == {"kind": "state"}

    delivery = await queue.receive()
    stats = await queue.stats()
    assert (stats.ready, stats.inflight) == (0, 1)
    processing = await queue.inspect("stateful")
    assert processing.state == MessageState.PROCESSING
    assert processing.attempts == 1
    assert processing.lease_until_ms > 0

    await delivery.ack()
    assert (await queue.inspect("stateful")).state == MessageState.DONE


async def test_delay_extend_and_old_token(queue):
    queue._options.poll_timeout = timedelta(milliseconds=20)
    queue._options.visibility_timeout = timedelta(milliseconds=50)
    await queue.enqueue(
        Message(payload=b"later"),
        id="later",
        delay=timedelta(milliseconds=70),
    )

    with pytest.raises(NoMessageError):
        await queue.receive()
    await asyncio.sleep(0.08)
    delivery = await queue.receive()
    with pytest.raises(LostOwnershipError):
        await queue.ack(delivery.id, "old-token")
    await delivery.extend(timedelta(milliseconds=100))
    await delivery.ack()


async def test_consume_emits_handler_latency(queue):
    events: list[Event] = []
    handled = asyncio.Event()

    class Recorder:
        def observe(self, event: Event) -> None:
            events.append(event)

    queue._options.observer = Recorder()
    await queue.enqueue(Message(payload=b"payload"))

    async def handler(_delivery) -> None:
        await asyncio.sleep(0.005)
        handled.set()

    task = asyncio.create_task(queue.consume(1, handler))
    await asyncio.wait_for(handled.wait(), timeout=1)
    while not any(
        event.operation == Operation.HANDLER and event.status == EventStatus.OK
        for event in events
    ):
        await asyncio.sleep(0.001)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    event = next(event for event in events if event.operation == Operation.HANDLER)
    assert event.duration > timedelta(0)


@pytest.mark.parametrize("handler_fails", [False, True])
async def test_consume_propagates_settlement_lost_ownership(queue, handler_fails):
    queue._options.visibility_timeout = timedelta(milliseconds=30)
    queue._options.retry_backoff = lambda _attempt: timedelta(0)
    started = asyncio.Event()
    release = asyncio.Event()
    await queue.enqueue(Message(payload=b"slow"), id="slow-settlement")

    async def handler(_delivery) -> None:
        started.set()
        await release.wait()
        if handler_fails:
            raise RuntimeError("handler failed")

    task = asyncio.create_task(queue.consume(1, handler))
    await asyncio.wait_for(started.wait(), timeout=1)
    await asyncio.sleep(0.05)
    assert await queue.reap_expired(10) == 1
    release.set()

    with pytest.raises(LostOwnershipError):
        await asyncio.wait_for(task, timeout=1)


async def test_consume_propagates_ack_error(queue):
    handled = asyncio.Event()
    await queue.enqueue(Message(payload=b"settlement"))
    queue.ack = AsyncMock(side_effect=RuntimeError("redis ack failed"))

    async def handler(_delivery) -> None:
        handled.set()

    with pytest.raises(RuntimeError, match="redis ack failed"):
        await asyncio.wait_for(queue.consume(1, handler), timeout=1)
    assert handled.is_set()


async def test_consume_with_signals_propagates_consumer_error():
    class BrokenQueue:
        async def consume(self, _concurrency, _handler) -> None:
            raise RuntimeError("consumer failed")

    async def handler(_delivery) -> None:
        pass

    with pytest.raises(RuntimeError, match="consumer failed"):
        await consume_with_signals(BrokenQueue(), 1, handler, signals=[])
