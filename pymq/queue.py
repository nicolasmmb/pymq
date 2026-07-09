"""Core Queue class — script loading, EVALSHA, enqueue, receive, consume, ack/nack/extend, reap.

Corresponds to gomq/queue.go — Queue struct, New(), Enqueue, Receive, Consume,
ack, nack, extend, ReapExpired, eval, evalSha, loadScripts, helpers.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import socket
import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable, get_type_hints

from pydantic import TypeAdapter
from redis.asyncio import Redis

from pymq.errors import (
    AlreadyDoneError,
    DLQError,
    DuplicateError,
    ErrorCode,
    GomqError,
    InvalidConfigError,
    LostOwnershipError,
    NoMessageError,
)
from pymq.event import Event, EventStatus, Hooks, Operation, Observer
from pymq.keys import RedisKeys, make_redis_keys
from pymq.message import Delivery, Message
from pymq.middleware import Handler, chain_consumer, chain_producer
from pymq.options import EnqueueOptions, NackOptions, Options

logger = logging.getLogger("pymq")

SCRIPTS_DIR = Path(__file__).parent / "scripts"
SCRIPT_NAMES = [
    "enqueue_one",
    "claim_one",
    "ack",
    "nack",
    "extend",
    "reap_expired",
    "stats",
    "inspect_message",
    "list_dlq",
    "requeue_dlq",
    "delete_dlq",
    "worker_heartbeat",
    "worker_list",
    "worker_control_set",
    "worker_control_get",
]


def _new_id() -> str:
    """Generate a UUID hex ID (32 chars).

    Mirrors gomq/queue.go newID().
    """
    return uuid.uuid4().hex


def _duration_ms(td: timedelta) -> int:
    """Convert timedelta to milliseconds.

    Mirrors gomq/queue.go durationMillis().
    """
    return int(td.total_seconds() * 1000)


def _is_no_script(err: Exception) -> bool:
    """Check if a Redis error is NOSCRIPT.

    Mirrors gomq/queue.go isNoScript().
    """
    return "NOSCRIPT" in str(err).upper()


def _hostname() -> str:
    """Get hostname, fallback to 'pymq-worker'.

    Mirrors gomq/managed.go hostname().
    """
    try:
        name = socket.gethostname()
        return name if name else "pymq-worker"
    except Exception:
        return "pymq-worker"


def _first_script_status(result: Any) -> str:
    """Extract the first element from a Lua script result array.

    Mirrors gomq/queue.go firstScriptStatus().
    """
    if isinstance(result, (list, tuple)) and len(result) > 0:
        v = result[0]
        return v.decode() if isinstance(v, bytes) else str(v)
    if isinstance(result, bytes):
        return result.decode()
    return str(result) if result is not None else ""


def _to_int(v: Any) -> int:
    """Coerce Redis response to int.

    Mirrors gomq/queue.go toInt().
    """
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, bytes):
        return int(v)
    if isinstance(v, str):
        return int(v)
    if isinstance(v, (list, tuple)) and len(v) > 0:
        return _to_int(v[0])
    return 0


def _to_float(v: Any) -> float:
    """Coerce Redis response to float.

    Mirrors gomq/admin.go toFloat().
    """
    if isinstance(v, float):
        return v
    if isinstance(v, (int, bytes, str)):
        return float(v)
    return 0.0


def _to_str(v: Any) -> str:
    """Coerce Redis response to str.

    Mirrors gomq/queue.go toString().
    """
    if v is None:
        return ""
    if isinstance(v, bytes):
        return v.decode()
    if isinstance(v, str):
        return v
    return str(v)


def _event_status_from_script(status: str) -> EventStatus:
    """Map a script status string to EventStatus."""
    try:
        return EventStatus(status)
    except ValueError:
        return EventStatus.UNEXPECTED


class Queue:
    """Redis-backed work queue with Lua-atomic state transitions.

    Corresponds to gomq/queue.go Queue struct.
    """

    def __init__(self, client: Redis, **kwargs: Any) -> None:
        if client is None:
            raise InvalidConfigError(op="new", message="redis client is required")
        opts_kwargs = {k: v for k, v in kwargs.items() if hasattr(Options, k)}
        if "queue_name" not in opts_kwargs:
            opts_kwargs["queue_name"] = kwargs.get("queue_name", "")
        self._client = client
        self._options = Options(**opts_kwargs)
        self._keys = make_redis_keys(self._options.queue_name)
        self._scripts: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._loaded = False
        self._noop_observer = _NoopObserver()

        if self._options.observer is None:
            self._options.observer = self._noop_observer
        if self._options.hooks is None:
            self._options.hooks = Hooks()

    @property
    def queue_name(self) -> str:
        return self._options.queue_name

    async def _ensure_scripts(self) -> None:
        """Load all Lua scripts into Redis, caching their SHA hashes.

        Mirrors gomq/queue.go loadScripts().
        """
        if self._loaded:
            return
        async with self._lock:
            if self._loaded:
                return
            loaded: dict[str, str] = {}
            for name in SCRIPT_NAMES:
                script_path = SCRIPTS_DIR / f"{name}.lua"
                body = script_path.read_text()
                sha = await self._client.script_load(body)
                loaded[name] = sha
            self._scripts = loaded
            self._loaded = True

    async def _eval(self, name: str, keys: list[str], *args: Any) -> Any:
        """Execute a Lua script via EVALSHA with NOSCRIPT retry.

        Mirrors gomq/queue.go eval() + evalSha().
        """
        await self._ensure_scripts()
        sha = self._scripts[name]
        try:
            result = await self._client.evalsha(sha, len(keys), *keys, *args)
            return result
        except Exception as e:
            if _is_no_script(e):
                await self._ensure_scripts()
                sha = self._scripts[name]
                return await self._client.evalsha(sha, len(keys), *keys, *args)
            raise

    def _emit(self, event: Event) -> None:
        """Emit an event to the observer and hooks.

        Mirrors gomq/queue.go emit() + emitObserver() + emitHook().
        Panic in observer or hook is caught and forwarded to on_error.
        """
        if not event.queue:
            event.queue = self._options.queue_name

        # Observer
        try:
            self._options.observer.observe(event)
        except Exception as recovered:
            try:
                self._emit_hook(
                    Event(
                        operation=event.operation,
                        status=EventStatus.OBSERVER_PANIC,
                        queue=event.queue,
                        message_id=event.message_id,
                        attempt=event.attempt,
                        error=Exception(f"pymq: observer panic: {recovered}"),
                    )
                )
            except Exception:
                pass

        # Hook
        self._emit_hook(event)

    def _emit_hook(self, event: Event) -> None:
        """Dispatch to the appropriate hook callback."""
        hooks: Hooks = self._options.hooks
        hook = None
        if event.error is not None:
            hook = hooks.on_error
        elif event.operation == Operation.CLAIM:
            hook = hooks.on_claim
        elif event.operation == Operation.ACK:
            hook = hooks.on_ack
        elif event.operation == Operation.RETRY:
            hook = hooks.on_retry
        elif event.operation == Operation.DLQ:
            hook = hooks.on_dlq
        if hook is None:
            return
        try:
            hook(event)
        except Exception as recovered:
            if event.error is None and hooks.on_error is not None:
                try:
                    hooks.on_error(
                        Event(
                            operation=event.operation,
                            status=EventStatus.HOOK_PANIC,
                            queue=event.queue,
                            message_id=event.message_id,
                            attempt=event.attempt,
                            error=Exception(f"pymq: hook panic: {recovered}"),
                        )
                    )
                except Exception:
                    pass

    # --- Public API ---

    async def enqueue(
        self,
        msg: Message,
        *,
        id: str = "",
        delay: timedelta | None = None,
    ) -> str:
        """Enqueue a message.

        If msg.payload is a Pydantic BaseModel or dataclass, it is serialized to
        JSON bytes automatically via TypeAdapter.

        Corresponds to gomq/queue.go Enqueue() + enqueue().
        """
        return await chain_producer(self._enqueue, self._options.producer_middlewares)(msg, id=id, delay=delay)

    async def _enqueue(
        self,
        msg: Message,
        *,
        id: str = "",
        delay: timedelta | None = None,
    ) -> str:
        start = asyncio.get_event_loop().time()
        msg_id = id or msg.id or _new_id()
        msg_delay = delay if delay is not None else msg.delay

        if msg_delay < timedelta(0):
            err = InvalidConfigError(op="enqueue", message="delay cannot be negative")
            self._emit(
                Event(operation=Operation.ENQUEUE, status=EventStatus.ERROR, message_id=msg_id, error=err)
            )
            raise err

        # Serialize payload
        payload_bytes = self._serialize_payload(msg.payload)

        # Serialize headers
        header_json = ""
        if msg.headers:
            header_json = json.dumps(msg.headers)

        result = await self._eval(
            "enqueue_one",
            [
                self._keys.ready,
                self._keys.done,
                self._keys.payloads,
                self._keys.headers,
                self._keys.states,
                self._keys.attempts,
                self._keys.tokens,
                self._keys.leases,
                self._keys.metadata,
            ],
            msg_id,
            payload_bytes,
            header_json,
            _duration_ms(msg_delay),
            _duration_ms(self._options.dedupe_ttl),
        )

        status = _first_script_status(result)
        duration = timedelta(seconds=asyncio.get_event_loop().time() - start)

        if status == "ok":
            self._emit(
                Event(
                    operation=Operation.ENQUEUE,
                    status=EventStatus.OK,
                    message_id=msg_id,
                    duration=duration,
                )
            )
            return msg_id
        if status == "duplicate":
            err = DuplicateError(
                op="enqueue",
                queue=self._options.queue_name,
                message_id=msg_id,
            )
            self._emit(
                Event(
                    operation=Operation.ENQUEUE,
                    status=EventStatus.DUPLICATE,
                    message_id=msg_id,
                    duration=duration,
                    error=err,
                )
            )
            raise err
        if status == "done":
            err = AlreadyDoneError(
                op="enqueue",
                queue=self._options.queue_name,
                message_id=msg_id,
            )
            self._emit(
                Event(
                    operation=Operation.ENQUEUE,
                    status=EventStatus.DONE,
                    message_id=msg_id,
                    duration=duration,
                    error=err,
                )
            )
            raise err

        err = GomqError(
            code=ErrorCode.UNEXPECTED,
            op="enqueue",
            queue=self._options.queue_name,
            message_id=msg_id,
            message=f"unexpected enqueue status {status!r}",
        )
        self._emit(
            Event(
                operation=Operation.ENQUEUE,
                status=_event_status_from_script(status),
                message_id=msg_id,
                duration=duration,
                error=err,
            )
        )
        raise err

    async def receive(self) -> Delivery[bytes]:
        """Receive a single message by polling.

        Returns a Delivery with raw bytes payload. The consume() method
        handles type inference and deserialization automatically.

        Corresponds to gomq/queue.go Receive().
        """
        deadline = 0.0
        if self._options.poll_timeout > timedelta(0):
            deadline = asyncio.get_event_loop().time() + self._options.poll_timeout.total_seconds()

        while True:
            try:
                d = await self._claim_once()
                return d
            except NoMessageError:
                pass

            now = asyncio.get_event_loop().time()
            if self._options.poll_timeout <= timedelta(0) or (deadline > 0 and now >= deadline):
                raise NoMessageError()

            remaining = deadline - now if deadline > 0 else self._options.poll_interval.total_seconds()
            sleep_for = min(self._options.poll_interval.total_seconds(), max(remaining, 0))
            await asyncio.sleep(sleep_for)

    async def _claim_once(self) -> Delivery[bytes]:
        """Single claim attempt — picks one ready message.

        Mirrors gomq/queue.go claimOnce().
        """
        start = asyncio.get_event_loop().time()
        token = _new_id()

        result = await self._eval(
            "claim_one",
            [
                self._keys.ready,
                self._keys.inflight,
                self._keys.payloads,
                self._keys.headers,
                self._keys.states,
                self._keys.attempts,
                self._keys.tokens,
                self._keys.leases,
                self._keys.metadata,
            ],
            _duration_ms(self._options.visibility_timeout),
            token,
            self._options.consumer_id,
        )

        status = _first_script_status(result)
        if status == "none" or not isinstance(result, (list, tuple)) or len(result) == 0:
            raise NoMessageError()

        if status != "ok" or len(result) < 7:
            err = GomqError(
                code=ErrorCode.UNEXPECTED,
                op="claim",
                queue=self._options.queue_name,
                message=f"unexpected claim result {result!r}",
            )
            self._emit(
                Event(
                    operation=Operation.CLAIM,
                    status=EventStatus.UNEXPECTED,
                    duration=timedelta(seconds=asyncio.get_event_loop().time() - start),
                    error=err,
                )
            )
            raise err

        # Parse headers
        headers: dict[str, str] = {}
        raw_headers = _to_str(result[3]) if len(result) > 3 else ""
        if raw_headers and raw_headers != "null":
            try:
                headers = json.loads(raw_headers)
            except (json.JSONDecodeError, TypeError):
                headers = {}

        d = Delivery[bytes](
            id=_to_str(result[1]),
            payload=(_to_str(result[2])).encode() if isinstance(result[2], str) else (result[2] or b""),
            headers=headers,
            token=_to_str(result[6]),
            attempts=_to_int(result[4]),
            lease_until_ms=_to_int(result[5]),
            _queue=self,
            _enqueued_at_ms=_to_int(result[7]) if len(result) >= 8 else 0,
        )

        self._emit(
            Event(
                operation=Operation.CLAIM,
                status=EventStatus.OK,
                message_id=d.id,
                attempt=d.attempts,
                duration=timedelta(seconds=asyncio.get_event_loop().time() - start),
            )
        )
        return d

    async def ack(self, id: str, token: str, enqueued_at_ms: int = 0) -> None:
        """Acknowledge a message (mark as done).

        Corresponds to gomq/queue.go ack().
        """
        start = asyncio.get_event_loop().time()
        result = await self._eval(
            "ack",
            [
                self._keys.inflight,
                self._keys.done,
                self._keys.payloads,
                self._keys.headers,
                self._keys.states,
                self._keys.attempts,
                self._keys.tokens,
                self._keys.leases,
                self._keys.metadata,
            ],
            id,
            token,
            _duration_ms(self._options.dedupe_ttl),
        )

        status = _first_script_status(result)
        duration = timedelta(seconds=asyncio.get_event_loop().time() - start)

        if status in ("ok", "already_done"):
            event = Event(
                operation=Operation.ACK,
                status=_event_status_from_script(status),
                message_id=id,
                duration=duration,
            )
            if enqueued_at_ms > 0:
                e2e_ms = int(asyncio.get_event_loop().time() * 1000) - enqueued_at_ms
                if e2e_ms > 0:
                    event.end_to_end_duration = timedelta(milliseconds=e2e_ms)
            self._emit(event)
            return

        if status == "lost_ownership":
            err = LostOwnershipError(
                op="ack",
                queue=self._options.queue_name,
                message_id=id,
            )
            self._emit(
                Event(
                    operation=Operation.ACK,
                    status=EventStatus.LOST_OWNERSHIP,
                    message_id=id,
                    duration=duration,
                    error=err,
                )
            )
            raise err

        err = GomqError(
            code=ErrorCode.UNEXPECTED,
            op="ack",
            queue=self._options.queue_name,
            message_id=id,
            message=f"unexpected ack status {status!r}",
        )
        self._emit(
            Event(
                operation=Operation.ACK,
                status=_event_status_from_script(status),
                message_id=id,
                duration=duration,
                error=err,
            )
        )
        raise err

    async def nack(
        self,
        id: str,
        token: str,
        attempt: int,
        *,
        permanent: bool = False,
        backoff: timedelta | None = None,
        error: Exception | None = None,
    ) -> None:
        """Reject a message for retry or DLQ.

        Corresponds to gomq/queue.go nack().
        """
        start = asyncio.get_event_loop().time()

        if backoff is None:
            backoff = self._options.retry_backoff(attempt)

        if backoff < timedelta(0):
            err = InvalidConfigError(op="nack", message="retry_backoff returned negative duration")
            self._emit(
                Event(
                    operation=Operation.NACK,
                    status=EventStatus.ERROR,
                    message_id=id,
                    attempt=attempt,
                    duration=timedelta(seconds=asyncio.get_event_loop().time() - start),
                    error=err,
                )
            )
            raise err

        err_msg = ""
        if error is not None:
            err_msg = str(error)

        max_attempts = 0 if permanent else self._options.max_attempts

        result = await self._eval(
            "nack",
            [
                self._keys.ready,
                self._keys.inflight,
                self._keys.dlq,
                self._keys.states,
                self._keys.attempts,
                self._keys.tokens,
                self._keys.leases,
                self._keys.errors,
            ],
            id,
            token,
            max_attempts,
            _duration_ms(backoff),
            err_msg,
        )

        status = _first_script_status(result)
        duration = timedelta(seconds=asyncio.get_event_loop().time() - start)

        if status == "retry":
            self._emit(
                Event(
                    operation=Operation.RETRY,
                    status=EventStatus.RETRY,
                    message_id=id,
                    attempt=attempt,
                    duration=duration,
                    error=error,
                )
            )
            return

        if status == "dlq":
            self._emit(
                Event(
                    operation=Operation.DLQ,
                    status=EventStatus.DLQ,
                    message_id=id,
                    attempt=attempt,
                    duration=duration,
                    error=DLQError(
                        op="nack",
                        queue=self._options.queue_name,
                        message_id=id,
                        cause=error,
                    ),
                )
            )
            return

        if status == "lost_ownership":
            err = LostOwnershipError(
                op="nack",
                queue=self._options.queue_name,
                message_id=id,
            )
            self._emit(
                Event(
                    operation=Operation.NACK,
                    status=EventStatus.LOST_OWNERSHIP,
                    message_id=id,
                    attempt=attempt,
                    duration=duration,
                    error=err,
                )
            )
            raise err

        err = GomqError(
            code=ErrorCode.UNEXPECTED,
            op="nack",
            queue=self._options.queue_name,
            message_id=id,
            message=f"unexpected nack status {status!r}",
        )
        self._emit(
            Event(
                operation=Operation.NACK,
                status=_event_status_from_script(status),
                message_id=id,
                attempt=attempt,
                duration=duration,
                error=err,
            )
        )
        raise err

    async def extend(self, id: str, token: str, timeout: timedelta) -> None:
        """Extend the visibility timeout (lease) for an in-flight message.

        Corresponds to gomq/queue.go extend().
        """
        if timeout <= timedelta(0):
            raise InvalidConfigError(op="extend", message="timeout must be positive")

        start = asyncio.get_event_loop().time()
        result = await self._eval(
            "extend",
            [
                self._keys.inflight,
                self._keys.tokens,
                self._keys.leases,
                self._keys.states,
            ],
            id,
            token,
            _duration_ms(timeout),
        )

        status = _first_script_status(result)
        duration = timedelta(seconds=asyncio.get_event_loop().time() - start)

        if status == "ok":
            self._emit(
                Event(operation=Operation.EXTEND, status=EventStatus.OK, message_id=id, duration=duration)
            )
            return

        if status == "lost_ownership":
            err = LostOwnershipError(
                op="extend",
                queue=self._options.queue_name,
                message_id=id,
            )
            self._emit(
                Event(
                    operation=Operation.EXTEND,
                    status=EventStatus.LOST_OWNERSHIP,
                    message_id=id,
                    duration=duration,
                    error=err,
                )
            )
            raise err

        err = GomqError(
            code=ErrorCode.UNEXPECTED,
            op="extend",
            queue=self._options.queue_name,
            message_id=id,
            message=f"unexpected extend status {status!r}",
        )
        self._emit(
            Event(
                operation=Operation.EXTEND,
                status=_event_status_from_script(status),
                message_id=id,
                duration=duration,
                error=err,
            )
        )
        raise err

    async def consume(self, concurrency: int, handler: Handler) -> None:
        """Consume messages with N concurrent workers.

        Inspects the handler's type annotation to infer the payload type T.
        If handler is `async def h(d: Delivery[Order])`, automatically
        deserializes bytes → Order via Pydantic TypeAdapter.

        Corresponds to gomq/queue.go Consume().
        """
        if concurrency <= 0:
            raise InvalidConfigError(op="consume", message="concurrency must be positive")
        if handler is None:
            raise InvalidConfigError(op="consume", message="handler is required")

        # Infer payload type from handler annotation
        payload_type = self._extract_payload_type(handler)

        # Build deserializer
        if payload_type is bytes:
            adapter = None
        else:
            adapter = TypeAdapter(payload_type)

        # Wrap handler to auto-deserialize
        async def raw_handler(raw: Delivery[bytes]) -> None:
            if adapter is not None:
                raw_bytes = raw.payload
                if isinstance(raw_bytes, str):
                    raw_bytes = raw_bytes.encode()
                typed_payload = adapter.validate_json(raw_bytes)
                typed_delivery = Delivery(
                    id=raw.id,
                    payload=typed_payload,
                    headers=raw.headers,
                    token=raw.token,
                    attempts=raw.attempts,
                    lease_until_ms=raw.lease_until_ms,
                    _queue=raw._queue,
                    _enqueued_at_ms=raw._enqueued_at_ms,
                )
                await handler(typed_delivery)
            else:
                await handler(raw)

        wrapped = chain_consumer(raw_handler, self._options.consumer_middlewares)

        # Spawn N worker tasks
        stop = asyncio.Event()
        tasks: list[asyncio.Task[None]] = []

        for i in range(concurrency):
            task = asyncio.create_task(self._worker_loop(i, wrapped, stop))
            tasks.append(task)

        # Wait until cancelled
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            stop.set()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _worker_loop(
        self, worker_id: int, handler: Handler, stop: asyncio.Event
    ) -> None:
        """Single worker loop: receive → handler → ack/nack."""
        while not stop.is_set():
            try:
                d = await self.receive()
            except NoMessageError:
                await asyncio.sleep(0.05)
                continue
            except Exception:
                if stop.is_set():
                    return
                continue

            start = asyncio.get_event_loop().time()
            try:
                await handler(d)
                self._emit(
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
                except Exception:
                    return
            except Exception as handler_err:
                self._emit(
                    Event(
                        operation=Operation.HANDLER,
                        status=EventStatus.ERROR,
                        message_id=d.id,
                        attempt=d.attempts,
                        duration=timedelta(seconds=asyncio.get_event_loop().time() - start),
                        error=handler_err,
                    )
                )
                self._emit(
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
                except Exception:
                    return

    async def reap_expired(self, limit: int = 100) -> int:
        """Reclaim expired in-flight messages.

        Returns the number of messages moved.

        Corresponds to gomq/queue.go ReapExpired().
        """
        if limit <= 0:
            limit = 100

        start = asyncio.get_event_loop().time()
        backoff = self._options.retry_backoff(self._options.max_attempts)

        result = await self._eval(
            "reap_expired",
            [
                self._keys.ready,
                self._keys.inflight,
                self._keys.dlq,
                self._keys.states,
                self._keys.attempts,
                self._keys.tokens,
                self._keys.leases,
                self._keys.errors,
            ],
            self._options.max_attempts,
            _duration_ms(backoff),
            limit,
        )

        duration = timedelta(seconds=asyncio.get_event_loop().time() - start)

        values = result if isinstance(result, (list, tuple)) else [result]
        moved = _to_int(values[0]) if len(values) > 0 else 0
        retried = _to_int(values[1]) if len(values) > 2 else 0
        dead = _to_int(values[2]) if len(values) > 2 else 0

        self._emit(Event(operation=Operation.REAP, status=EventStatus.OK, duration=duration))
        if retried > 0:
            self._emit(
                Event(operation=Operation.RETRY, status=EventStatus.REAPED, duration=duration)
            )
        if dead > 0:
            self._emit(
                Event(
                    operation=Operation.DLQ,
                    status=EventStatus.REAPED,
                    duration=duration,
                    error=DLQError(op="reap", queue=self._options.queue_name),
                )
            )

        return moved

    def _extract_payload_type(self, handler: Handler) -> type:
        """Extract T from Delivery[T] in handler's type annotation.

        Uses inspect.signature + get_type_hints to read the first parameter's
        annotation at runtime. Falls back to bytes if no annotation found.
        """
        try:
            hints = get_type_hints(handler)
        except Exception:
            return bytes

        delivery_type = None
        # Try first positional parameter
        sig = inspect.signature(handler)
        params = list(sig.parameters.values())
        if params:
            first_param = params[0]
            delivery_type = hints.get(first_param.name)

        if delivery_type is None:
            return bytes

        # Extract T from Delivery[T]
        origin = getattr(delivery_type, "__origin__", None)
        args = getattr(delivery_type, "__args__", None)

        # Check if it's Delivery[...] — Delivery is imported from pymq.message
        from pymq.message import Delivery as DeliveryClass

        if origin is DeliveryClass and args:
            return args[0]

        return bytes

    def _serialize_payload(self, payload: Any) -> bytes:
        """Serialize payload to bytes for Redis storage.

        If payload is already bytes, return as-is.
        If payload is a Pydantic BaseModel, serialize to JSON.
        Otherwise, try json.dumps.
        """
        if isinstance(payload, bytes):
            return payload
        if isinstance(payload, str):
            return payload.encode()
        # Try Pydantic model_dump_json
        if hasattr(payload, "model_dump_json"):
            return payload.model_dump_json().encode()
        # Try dataclass
        from dataclasses import asdict, is_dataclass

        if is_dataclass(payload) and not isinstance(payload, type):
            return json.dumps(asdict(payload)).encode()
        # Fallback
        return json.dumps(payload).encode()


class _NoopObserver:
    """Default no-op observer."""

    def observe(self, event: Event) -> None:
        pass
