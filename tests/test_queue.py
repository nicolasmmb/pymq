"""Integration tests for pymq.queue — requires Redis."""

import asyncio
from datetime import timedelta

import pytest
from pydantic import BaseModel

from pymq.errors import DuplicateError, NoMessageError
from pymq.message import Delivery, Message
from pymq.options import AckRetentionPolicy, ListOptions
from pymq.queue import Queue


class Order(BaseModel):
    item: str
    qty: int


class TestEnqueueClaimAck:
    """Basic enqueue → claim → ack cycle."""

    async def test_enqueue_returns_id(self, queue: Queue):
        msg_id = await queue.enqueue(Message(payload=b"hello"))
        assert isinstance(msg_id, str)
        assert len(msg_id) == 32

    async def test_enqueue_with_custom_id(self, queue: Queue):
        msg_id = await queue.enqueue(Message(payload=b"hello"), id="my-id-123")
        assert msg_id == "my-id-123"

    async def test_claim_returns_delivery(self, queue: Queue):
        await queue.enqueue(Message(payload=b"world"))
        d = await queue._claim_once()
        assert d.id
        assert d.payload == b"world"
        assert d.token
        assert d.attempts >= 1

    async def test_ack_removes_message(self, queue: Queue):
        await queue.enqueue(Message(payload=b"done"))
        d = await queue._claim_once()
        await queue.ack(d.id, d.token)
        # No more messages
        with pytest.raises(NoMessageError):
            await queue._claim_once()

    async def test_full_cycle(self, queue: Queue):
        await queue.enqueue(Message(payload=b"cycle"))
        d = await queue.receive()
        assert d.payload == b"cycle"
        await d.ack()
        with pytest.raises(NoMessageError):
            await queue._claim_once()


class TestHeaders:
    """Message headers survive round-trip."""

    async def test_headers_persist(self, queue: Queue):
        await queue.enqueue(Message(payload=b"h", headers={"x-trace": "abc"}))
        d = await queue._claim_once()
        assert d.headers == {"x-trace": "abc"}


class TestNackRetry:
    """Nack puts message back for retry."""

    async def test_nack_retry(self, queue: Queue):
        await queue.enqueue(Message(payload=b"retry-me"))
        d = await queue._claim_once()
        await queue.nack(d.id, d.token, d.attempts, backoff=timedelta(0))
        # Message should be claimable again immediately
        d2 = await queue._claim_once()
        assert d2.payload == b"retry-me"
        assert d2.attempts >= 2
        await d2.ack()


class TestExtend:
    """Extend refreshes lease."""

    async def test_extend_ok(self, queue: Queue):
        await queue.enqueue(Message(payload=b"ext"))
        d = await queue._claim_once()
        # Should not raise
        await queue.extend(d.id, d.token, timedelta(seconds=30))
        await d.ack()


class TestDuplicateDetection:
    """Same ID twice raises DuplicateError."""

    async def test_duplicate(self, queue: Queue):
        await queue.enqueue(Message(payload=b"a"), id="dup-1")
        with pytest.raises(DuplicateError):
            await queue.enqueue(Message(payload=b"b"), id="dup-1")


class TestDoneRetention:
    async def test_ack_retains_completed_job(self, queue: Queue):
        await queue.enqueue(
            Message(payload=b"keep", headers={"kind": "retained"}),
            id="keep",
        )
        delivery = await queue._claim_once()
        await queue._client.hset(queue._keys.errors, delivery.id, "previous retry")
        await delivery.ack()

        info = await queue.inspect("keep")
        assert info.state.value == "done"
        assert info.payload == b"keep"
        assert info.headers == {"kind": "retained"}
        assert info.attempts == 1
        assert info.last_error == "previous retry"
        assert info.done_score > 0

        items = await queue.list_done(ListOptions(count=10))
        assert len(items) == 1
        assert items[0].id == "keep"
        assert items[0].payload == b"keep"

    async def test_delete_acked_keeps_only_marker(self, queue: Queue):
        queue._options.ack_retention_policy = AckRetentionPolicy.DELETE_ACKED
        await queue.enqueue(Message(payload=b"drop"), id="drop")
        delivery = await queue._claim_once()
        await delivery.ack()

        info = await queue.inspect("drop")
        assert info.state.value == "done"
        assert info.payload == b""
        assert info.done_score > 0

    async def test_delete_and_prune_done(self, queue: Queue):
        await queue.enqueue(Message(payload=b"delete"), id="delete")
        delivery = await queue._claim_once()
        await delivery.ack()
        await queue.delete_done("delete")
        with pytest.raises(NoMessageError):
            await queue.inspect("delete")
        assert await queue.enqueue(Message(payload=b"reused"), id="delete") == "delete"

        delivery = await queue._claim_once()
        await delivery.ack()
        assert await queue.prune_done(timedelta(0), 100) == 1
        with pytest.raises(NoMessageError):
            await queue.inspect("delete")

    async def test_id_reuse_replaces_retained_record(self, queue: Queue):
        queue._options.dedupe_ttl = timedelta(milliseconds=1)
        await queue.enqueue(Message(payload=b"old"), id="same")
        delivery = await queue._claim_once()
        await delivery.ack()
        await asyncio.sleep(0.01)

        await queue.enqueue(Message(payload=b"new"), id="same")
        delivery = await queue._claim_once()
        assert delivery.payload == b"new"

    async def test_reaper_prunes_done_records(self, queue: Queue):
        from pymq.lifecycle import run_reaper

        queue._options.done_retention_ttl = timedelta(milliseconds=1)
        await queue.enqueue(Message(payload=b"old"), id="old")
        delivery = await queue._claim_once()
        await delivery.ack()
        await asyncio.sleep(0.01)

        task = asyncio.create_task(run_reaper(queue, timedelta(milliseconds=1)))
        try:
            for _ in range(100):
                try:
                    await queue.inspect("old")
                except NoMessageError:
                    break
                await asyncio.sleep(0.001)
            else:
                pytest.fail("RunReaper did not prune the completed job")
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task


class TestPydanticSerialization:
    """Pydantic models serialize/deserialize through the queue."""

    async def test_roundtrip(self, queue: Queue):
        order = Order(item="widget", qty=5)
        await queue.enqueue(Message(payload=order))
        d = await queue._claim_once()
        # Raw bytes are JSON
        import json

        data = json.loads(d.payload)
        assert data["item"] == "widget"
        assert data["qty"] == 5
        await d.ack()

    async def test_consume_infers_type(self, queue: Queue):
        """consume() inspects Delivery[Order] and deserializes automatically."""
        order = Order(item="gadget", qty=3)
        await queue.enqueue(Message(payload=order))

        received: list[Order] = []

        async def handler(d: Delivery[Order]):
            received.append(d.payload)
            await d.ack()

        # Run consume for a short time
        task = asyncio.create_task(queue.consume(1, handler))
        await asyncio.sleep(0.5)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert len(received) == 1
        assert received[0].item == "gadget"
        assert received[0].qty == 3


class TestReapExpired:
    """Reap expired messages back to ready."""

    async def test_reap(self, queue: Queue):
        # Use a very short visibility timeout
        queue._options.visibility_timeout = timedelta(seconds=1)
        await queue.enqueue(Message(payload=b"will-expire"))
        await queue._claim_once()
        # Wait for lease to expire
        await asyncio.sleep(1.5)
        moved = await queue.reap_expired()
        assert moved >= 1
        # Message should be claimable again (use receive with poll to handle backoff)
        queue._options.poll_timeout = timedelta(seconds=10)
        queue._options.poll_interval = timedelta(milliseconds=200)
        d2 = await queue.receive()
        assert d2.payload == b"will-expire"
        await d2.ack()
