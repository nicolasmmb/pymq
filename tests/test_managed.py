"""Integration tests for managed consumers and lifecycle helpers."""

import asyncio
import time
from datetime import timedelta

import pytest

from pymq import MANAGED_WORKER_ALL, ManagedConsumerOptions
from pymq.errors import InvalidConfigError
from pymq.queue import Queue


async def _wait_for_worker(
    queue: Queue,
    worker_id: str,
    predicate,
    *,
    timeout: float = 2,
):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        workers = await queue.list_managed_workers()
        worker = next((item for item in workers if item.id == worker_id), None)
        if worker is not None and predicate(worker):
            return worker
        await asyncio.sleep(0.01)
    pytest.fail(f"managed worker {worker_id!r} did not reach the expected state")


async def test_managed_consumer_control_and_shutdown(queue: Queue):
    async def handler(_delivery) -> None:
        pass

    options = ManagedConsumerOptions(
        worker_id="managed-test",
        concurrency=2,
        heartbeat_interval=timedelta(milliseconds=20),
        heartbeat_ttl=timedelta(seconds=1),
    )
    task = asyncio.create_task(queue.run_managed_consumer(options, handler))

    try:
        worker = await _wait_for_worker(
            queue,
            options.worker_id,
            lambda item: item.state == "running" and item.current_concurrency == 2,
        )
        assert abs(worker.last_seen_ms - int(time.time() * 1000)) < 2_000

        await queue.set_managed_concurrency(1, options.worker_id)
        await _wait_for_worker(
            queue,
            options.worker_id,
            lambda item: item.current_concurrency == 1
            and item.desired_concurrency == 1,
        )

        await queue.pause_managed_workers(options.worker_id)
        await _wait_for_worker(
            queue,
            options.worker_id,
            lambda item: item.state == "paused" and item.current_concurrency == 0,
        )

        await queue.resume_managed_workers(options.worker_id)
        await _wait_for_worker(
            queue,
            options.worker_id,
            lambda item: item.state == "running" and item.current_concurrency == 1,
        )
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_managed_defaults_and_public_constant(queue: Queue):
    assert MANAGED_WORKER_ALL == "__all__"
    options = ManagedConsumerOptions(
        worker_id="managed-default",
        concurrency=0,
        heartbeat_interval=timedelta(milliseconds=20),
        heartbeat_ttl=timedelta(seconds=1),
    )

    async def handler(_delivery) -> None:
        pass

    task = asyncio.create_task(queue.run_managed_consumer(options, handler))
    try:
        await _wait_for_worker(
            queue,
            options.worker_id,
            lambda item: item.current_concurrency == 1,
        )
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_managed_negative_concurrency_is_invalid(queue: Queue):
    async def handler(_delivery) -> None:
        pass

    with pytest.raises(InvalidConfigError):
        await queue.run_managed_consumer(
            ManagedConsumerOptions(concurrency=-1),
            handler,
        )
