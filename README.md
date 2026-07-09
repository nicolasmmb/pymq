# pymq

Redis-backed work queue for Python. Port of [gomq](https://github.com/nicolasmmb/gomq).

At-least-once delivery with Lua-atomic state transitions, visibility timeouts, retries, DLQ, fencing tokens, middlewares, and typed Pydantic payloads.

## Install

```bash
pip install pymq
```

Requires Python 3.10+, Redis 6+.

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
    await d.ack()

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
)
```

## Ack / Nack / Extend

```python
d = await queue.receive()

# Success
await d.ack()

# Retry (goes back to ready queue)
await d.nack(attempt=d.attempts)

# Permanent failure (sends to DLQ)
await d.nack(attempt=d.attempts, permanent=True)

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

## Managed Consumer (with signal handling)

```python
from pymq import consume_with_signals

async def handle(d: Delivery[Order]):
    await d.ack()

await consume_with_signals(redis, "orders", 4, handle)
```

## Testing

```bash
# Unit tests (no Redis needed)
pytest tests/test_errors.py tests/test_state.py tests/test_keys.py tests/test_options.py tests/test_middleware.py -v

# Integration tests (needs Redis on localhost:6379)
pytest tests/test_queue.py -v
```

## License

MIT
