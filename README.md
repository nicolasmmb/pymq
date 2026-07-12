# pymq

Redis-backed work queue for Python. Port of [gomq](https://github.com/nicolasmmb/gomq).

At-least-once delivery with Lua-atomic state transitions, visibility timeouts, retries, DLQ, fencing tokens, middlewares, and typed Pydantic payloads.

## Install

```bash
pip install pymq
pip install 'pymq[prometheus]'  # optional
pip install 'pymq[otel]'        # optional
```

Requires Python 3.11+, Redis 6+.

## Quick Start

```python
import asyncio
from redis.asyncio import Redis
from pymq import Queue, Message, Delivery

async def main():
    redis = Redis.from_url("redis://localhost:6379")
    queue = Queue(redis, queue_name="orders")

    # Enqueue
    await queue.enqueue(Message(payload=b'{"item":"widget","qty":5}'))

    # Receive & ack
    d = await queue.receive()
    print(d.payload)  # b'{"item":"widget","qty":5}'
    await d.ack()

asyncio.run(main())
```

## Typed Payloads (Pydantic)

```python
from pydantic import BaseModel
from pymq import Queue, Message, Delivery

class Order(BaseModel):
    item: str
    qty: int

queue = Queue(redis, queue_name="typed")

# Enqueue a Pydantic model
await queue.enqueue(Message(payload=Order(item="widget", qty=5)))

# consume() auto-infers the type from the handler annotation
async def handle(d: Delivery[Order]):
    print(d.payload.item)  # typed!

await queue.consume(4, handle)
```

## Options

```python
from datetime import timedelta
from pymq import Queue, Options

queue = Queue(
    redis,
    queue_name="orders",
    visibility_timeout=timedelta(seconds=30),
    max_attempts=5,
    poll_timeout=timedelta(seconds=10),
    done_retention_ttl=timedelta(days=7),
)
```

## Retention and dedupe

Completed jobs retain payload, headers, attempts, and the last error by default.
Use `AckRetentionPolicy.DELETE_ACKED` to keep only the completed marker.

```python
from datetime import timedelta
from pymq import AckRetentionPolicy, ListOptions

queue = Queue(
    redis,
    queue_name="orders",
    done_retention_ttl=timedelta(days=7),
    ack_retention_policy=AckRetentionPolicy.RETAIN_ACKED,
)

info = await queue.inspect("order-123")
completed = await queue.list_done(ListOptions(count=100))
await queue.delete_done("order-123")
await queue.prune_done(timedelta(days=7), limit=1000)
```

`run_reaper()` removes records older than `done_retention_ttl`; a zero TTL
retains them indefinitely. `dedupe_ttl` remains independent, so a reused ID
after its dedupe period replaces its earlier retained record.

## DLQ administration

```python
from pymq import ListOptions, RequeueOptions

failed = await queue.list_dlq(ListOptions(count=100))
await queue.requeue_dlq("job-123", RequeueOptions(reset_attempts=True))
await queue.delete_dlq("job-123")
```

## Ack / Nack / Extend

```python
d = await queue.receive()

# Success
await d.ack()

# Retry (goes back to ready queue)
await d.nack()

# Permanent failure (sends to DLQ)
await d.nack(permanent=True)

# Extend lease (for long processing)
await queue.extend(d.id, d.token, timedelta(seconds=60))
```

## Hooks & Observability

```python
from pymq import Hooks, Event

def on_claim(e: Event):
    print(f"claimed {e.message_id}")

queue = Queue(
    redis,
    queue_name="orders",
    hooks=Hooks(on_claim=on_claim),
)
```

Prometheus and OpenTelemetry adapters use the same metric names and labels as
`gomq` and remain optional:

```python
from prometheus_client import CollectorRegistry
from pymq.observability.prometheus import Observer

registry = CollectorRegistry()
queue = Queue(redis, queue_name="orders", observer=Observer(registry))
```

## Managed consumers and signal handling

```python
from datetime import timedelta
from pymq import ManagedConsumerOptions

async def handle(d: Delivery[Order]):
    print(d.payload.item)

await queue.run_managed_consumer(
    ManagedConsumerOptions(
        worker_id="orders-1",
        concurrency=4,
        heartbeat_interval=timedelta(seconds=5),
    ),
    handle,
)

# Or run a regular consumer until SIGINT/SIGTERM:
await queue.consume_with_signals(4, handle)
```

Managed workers can be controlled at runtime:

```python
workers = await queue.list_managed_workers()
await queue.set_managed_concurrency(2, "orders-1")
await queue.pause_managed_workers("orders-1")
await queue.resume_managed_workers("orders-1")
```

## Testing

```bash
# Full suite (integration tests need Redis on localhost:6379)
uv run --extra prometheus --extra otel pytest
uv run --extra prometheus --extra otel ruff check pymq tests
uv run --extra prometheus --extra otel pyright
```

## License

MIT
