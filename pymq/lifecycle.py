"""Lifecycle helpers — run_reaper, consume_with_signals.

Corresponds to gomq/lifecycle.go — RunReaper, ConsumeWithSignals.
"""

from __future__ import annotations

import asyncio
import signal
from datetime import timedelta
from typing import Any

from pymq.errors import InvalidConfigError
from pymq.event import Event, EventStatus, Operation


async def run_reaper(queue: Any, interval: timedelta, limit: int = 100) -> None:
    """Run the reaper loop that reclaims expired in-flight messages.

    Corresponds to gomq/lifecycle.go RunReaper().
    Blocks until the event loop is cancelled.
    """
    if interval <= timedelta(0):
        raise InvalidConfigError(op="run_reaper", message="interval must be positive")
    if limit <= 0:
        raise InvalidConfigError(op="run_reaper", message="limit must be positive")

    while True:
        await asyncio.sleep(interval.total_seconds())
        try:
            await queue.reap_expired(limit)
        except Exception as err:
            queue._emit(Event(operation=Operation.REAP, status=EventStatus.ERROR, error=err))
        if queue._options.done_retention_ttl > timedelta(0):
            try:
                await queue.prune_done(queue._options.done_retention_ttl, limit)
            except Exception as err:
                queue._emit(Event(operation=Operation.ADMIN, status=EventStatus.ERROR, error=err))


async def consume_with_signals(
    queue: Any,
    concurrency: int,
    handler: Any,
    signals: list[int] | None = None,
) -> None:
    """Consume messages with graceful shutdown on signals.

    Corresponds to gomq/lifecycle.go ConsumeWithSignals().
    Listens for SIGINT/SIGTERM by default.
    """
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    if signals is None:
        signals = [signal.SIGINT, signal.SIGTERM]

    def _signal_handler() -> None:
        stop.set()

    for sig in signals:
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except (NotImplementedError, OSError):
            pass  # Windows doesn't support add_signal_handler

    task = asyncio.create_task(queue.consume(concurrency, handler))

    # Wait for stop signal or task completion
    done, _pending = await asyncio.wait(
        [asyncio.create_task(stop.wait()), task],
        return_when=asyncio.FIRST_COMPLETED,
    )

    # Cancel remaining
    for t in _pending:
        t.cancel()
    await asyncio.gather(*_pending, return_exceptions=True)

    # Remove signal handlers
    for sig in signals:
        try:
            loop.remove_signal_handler(sig)
        except (NotImplementedError, OSError):
            pass
