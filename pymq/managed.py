"""Managed consumer — worker pool with heartbeat and runtime control.

Corresponds to gomq/managed.go — RunManagedConsumer, ManagedWorkerInfo,
ManagedControl, ListManagedWorkers, Pause/Resume/SetConcurrency.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from pymq.errors import InvalidConfigError
from pymq.event import Event, EventStatus, Operation
from pymq.middleware import Handler, chain_consumer

MANAGED_WORKER_ALL = "__all__"


@dataclass
class ManagedConsumerOptions:
    """Options for run_managed_consumer.

    Corresponds to gomq/managed.go ManagedConsumerOptions.
    """

    worker_id: str = ""
    concurrency: int = 1
    heartbeat_interval: timedelta = field(default=timedelta(seconds=5))
    heartbeat_ttl: timedelta | None = None


@dataclass
class ManagedWorkerInfo:
    """Worker info published to Redis.

    Corresponds to gomq/managed.go ManagedWorkerInfo.
    """

    id: str = ""
    queue: str = ""
    hostname: str = ""
    pid: int = 0
    state: str = "running"
    current_concurrency: int = 0
    desired_concurrency: int = 0
    last_seen_ms: int = 0
    expires_at_ms: int = 0


@dataclass
class ManagedControl:
    """Control command read from Redis.

    Corresponds to gomq/managed.go ManagedControl.
    """

    paused: bool | None = None
    concurrency: int | None = None
    updated_at_ms: int = 0


class _ManagedWorker:
    """Internal worker wrapper with retire signal."""

    def __init__(self) -> None:
        self.retire = asyncio.Event()
        self._stopped = False

    def stop(self) -> None:
        if not self._stopped:
            self._stopped = True
            self.retire.set()


async def run_managed_consumer(
    queue: Any,
    opts: ManagedConsumerOptions,
    handler: Handler,
) -> None:
    """Run a managed consumer with runtime-controllable worker pool.

    Corresponds to gomq/managed.go RunManagedConsumer().
    """
    if handler is None:
        raise InvalidConfigError(op="managed_consume", message="handler is required")

    # Normalize options
    worker_id = opts.worker_id
    if not worker_id:
        worker_id = queue._options.consumer_id
    if not worker_id or worker_id == "pymq-consumer":
        worker_id = f"{_hostname()}-{os.getpid()}"
    if opts.concurrency <= 0:
        opts.concurrency = 1
    heartbeat_interval = opts.heartbeat_interval or timedelta(seconds=5)
    heartbeat_ttl = opts.heartbeat_ttl or heartbeat_interval * 3

    handler = chain_consumer(handler, queue._options.consumer_middlewares)

    managed_ctx = asyncio.Event()
    workers: dict[int, _ManagedWorker] = {}
    next_worker_id = 0
    done_queue: asyncio.Queue[int] = asyncio.Queue()
    err_queue: asyncio.Queue[Exception] = asyncio.Queue()

    desired = opts.concurrency
    paused = False

    def reconcile() -> None:
        nonlocal desired, paused
        if paused:
            for w in workers.values():
                w.stop()
            return
        while len(workers) > desired:
            for w in workers.values():
                w.stop()
                break
            break
        while len(workers) < desired:
            nonlocal next_worker_id
            next_worker_id += 1
            wid = next_worker_id
            w = _ManagedWorker()
            workers[wid] = w
            asyncio.create_task(_run_managed_worker(queue, w.retire, wid, handler, done_queue, err_queue))

    def apply_control(control: ManagedControl) -> None:
        nonlocal desired, paused
        if control.concurrency is not None and control.concurrency > 0:
            desired = control.concurrency
        if control.paused is not None:
            paused = control.paused
        reconcile()

    async def heartbeat() -> None:
        state = "paused" if paused else "running"
        info = ManagedWorkerInfo(
            id=worker_id,
            queue=queue.queue_name,
            hostname=_hostname(),
            pid=os.getpid(),
            state=state,
            current_concurrency=len(workers),
            desired_concurrency=desired,
            last_seen_ms=int(asyncio.get_event_loop().time() * 1000),
        )
        await _heartbeat_managed_worker(queue, info, heartbeat_ttl)

    reconcile()
    await heartbeat()

    ticker = asyncio.create_task(_ticker(heartbeat_interval))

    try:
        while not managed_ctx.is_set():
            done, _, pending = await asyncio.wait(
                [
                    asyncio.create_task(done_queue.get()),
                    asyncio.create_task(err_queue.get()),
                    asyncio.create_task(ticker),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                result = task.result()
                if isinstance(result, int):
                    workers.pop(result, None)
                    reconcile()
                elif isinstance(result, Exception):
                    raise result

            for task in pending:
                if task is ticker:
                    pass
                else:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

            if ticker in done:
                try:
                    control = await _get_managed_control(queue, worker_id)
                    apply_control(control)
                except Exception:
                    pass
                await heartbeat()
                ticker = asyncio.create_task(_ticker(heartbeat_interval))
    finally:
        ticker.cancel()
        for w in workers.values():
            w.stop()
        while not done_queue.empty():
            done_queue.get_nowait()


async def _ticker(interval: timedelta) -> None:
    await asyncio.sleep(interval.total_seconds())


async def _run_managed_worker(
    queue: Any,
    retire: asyncio.Event,
    worker_id: int,
    handler: Handler,
    done_queue: asyncio.Queue[int],
    err_queue: asyncio.Queue[Exception],
) -> None:
    """Single managed worker loop."""
    try:
        while not retire.is_set():
            receive_task = asyncio.create_task(queue.receive())
            retire_task = asyncio.create_task(retire.wait())

            done, pending = await asyncio.wait(
                [receive_task, retire_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

            if retire_task in done:
                return

            d = receive_task.result()
            if d is None:
                continue

            start = asyncio.get_event_loop().time()
            try:
                await handler(d)
                queue._emit(
                    Event(
                        operation=Operation.HANDLER,
                        status=EventStatus.OK,
                        message_id=d.id,
                        attempt=d.attempts,
                        duration=timedelta(seconds=asyncio.get_event_loop().time() - start),
                    )
                )
                try:
                    await d.ack()
                except Exception as err:
                    await err_queue.put(err)
                    return
            except Exception as handler_err:
                queue._emit(
                    Event(
                        operation=Operation.HANDLER,
                        status=EventStatus.ERROR,
                        message_id=d.id,
                        attempt=d.attempts,
                        duration=timedelta(seconds=asyncio.get_event_loop().time() - start),
                        error=handler_err,
                    )
                )
                queue._emit(
                    Event(
                        operation=Operation.NACK,
                        status=EventStatus.HANDLER_ERROR,
                        message_id=d.id,
                        attempt=d.attempts,
                        error=handler_err,
                    )
                )
                try:
                    from pymq.errors import is_permanent

                    nack_kwargs: dict[str, Any] = {"error": handler_err}
                    if is_permanent(handler_err):
                        nack_kwargs["permanent"] = True
                    await d.nack(**nack_kwargs)
                except Exception as nack_err:
                    await err_queue.put(nack_err)
                    return
    finally:
        await done_queue.put(worker_id)


async def _heartbeat_managed_worker(
    queue: Any, info: ManagedWorkerInfo, ttl: timedelta
) -> None:
    """Publish worker heartbeat to Redis."""
    body = json.dumps(
        {
            "id": info.id,
            "queue": info.queue,
            "hostname": info.hostname,
            "pid": info.pid,
            "state": info.state,
            "current_concurrency": info.current_concurrency,
            "desired_concurrency": info.desired_concurrency,
            "last_seen_ms": info.last_seen_ms,
        }
    )
    from pymq.queue import _duration_ms

    await queue._eval(
        "worker_heartbeat",
        [queue._keys.workers, queue._keys.worker_info],
        info.id,
        body,
        _duration_ms(ttl),
    )


async def _get_managed_control(queue: Any, worker_id: str) -> ManagedControl:
    """Read control commands from Redis."""
    result = await queue._eval(
        "worker_control_get",
        [queue._keys.worker_controls],
        worker_id,
        MANAGED_WORKER_ALL,
    )
    if not isinstance(result, (list, tuple)) or len(result) < 2:
        return ManagedControl()

    global_json = _to_str(result[0])
    specific_json = _to_str(result[1])

    control = ManagedControl()
    if global_json:
        try:
            data = json.loads(global_json)
            if "paused" in data:
                control.paused = data["paused"]
            if "concurrency" in data:
                control.concurrency = data["concurrency"]
            if "updated_at_ms" in data:
                control.updated_at_ms = data["updated_at_ms"]
        except (json.JSONDecodeError, TypeError):
            pass

    if specific_json:
        try:
            data = json.loads(specific_json)
            if "paused" in data:
                control.paused = data["paused"]
            if "concurrency" in data:
                control.concurrency = data["concurrency"]
            if "updated_at_ms" in data and data["updated_at_ms"] > control.updated_at_ms:
                control.updated_at_ms = data["updated_at_ms"]
        except (json.JSONDecodeError, TypeError):
            pass

    return control


def _to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bytes):
        return v.decode()
    if isinstance(v, str):
        return v
    return str(v)


def _hostname() -> str:
    try:
        name = socket.gethostname()
        return name if name else "pymq-worker"
    except Exception:
        return "pymq-worker"


# Admin operations on Queue
async def list_managed_workers(queue: Any) -> list[ManagedWorkerInfo]:
    """List all live workers.

    Corresponds to gomq/managed.go ListManagedWorkers().
    """
    result = await queue._eval(
        "worker_list",
        [queue._keys.workers, queue._keys.worker_info],
    )
    if not isinstance(result, (list, tuple)):
        return []

    workers: list[ManagedWorkerInfo] = []
    for row in result:
        if not isinstance(row, (list, tuple)) or len(row) < 3:
            continue
        info = ManagedWorkerInfo()
        raw = _to_str(row[2])
        if raw:
            try:
                data = json.loads(raw)
                info = ManagedWorkerInfo(**data)
            except (json.JSONDecodeError, TypeError):
                pass
        if not info.id:
            info.id = _to_str(row[0])
        info.expires_at_ms = int(row[1]) if isinstance(row[1], (int, float)) else int(_to_str(row[1]))
        workers.append(info)
    return workers


async def pause_managed_workers(queue: Any, target: str = MANAGED_WORKER_ALL) -> None:
    """Pause workers.

    Corresponds to gomq/managed.go PauseManagedWorkers().
    """
    control = json.dumps({"paused": True, "updated_at_ms": int(asyncio.get_event_loop().time() * 1000)})
    await queue._eval(
        "worker_control_set",
        [queue._keys.worker_controls],
        target or MANAGED_WORKER_ALL,
        control,
    )


async def resume_managed_workers(queue: Any, target: str = MANAGED_WORKER_ALL) -> None:
    """Resume workers.

    Corresponds to gomq/managed.go ResumeManagedWorkers().
    """
    control = json.dumps({"paused": False, "updated_at_ms": int(asyncio.get_event_loop().time() * 1000)})
    await queue._eval(
        "worker_control_set",
        [queue._keys.worker_controls],
        target or MANAGED_WORKER_ALL,
        control,
    )


async def set_managed_concurrency(
    queue: Any, concurrency: int, target: str = MANAGED_WORKER_ALL
) -> None:
    """Set desired concurrency for workers.

    Corresponds to gomq/managed.go SetManagedConcurrency().
    """
    if concurrency <= 0:
        raise InvalidConfigError(op="worker_set_concurrency", message="concurrency must be positive")
    control = json.dumps(
        {"concurrency": concurrency, "updated_at_ms": int(asyncio.get_event_loop().time() * 1000)}
    )
    await queue._eval(
        "worker_control_set",
        [queue._keys.worker_controls],
        target or MANAGED_WORKER_ALL,
        control,
    )
