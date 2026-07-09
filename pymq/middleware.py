"""Handler type and middleware for consumer/producer.

Corresponds to gomq/middleware.go — Handler, ConsumerMiddleware, ProducerMiddleware, chain functions.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, TypeVar

T = TypeVar("T")

# A handler receives a typed delivery, raises on error (no return value).
# Corresponds to gomq/middleware.go Handler type.
Handler = Callable[..., Awaitable[None]]

# Consumer middleware wraps a handler function.
# Corresponds to gomq/middleware.go ConsumerMiddleware type.
ConsumerMiddleware = Callable[[Handler], Handler]

# Producer middleware wraps the enqueue function.
# Corresponds to gomq/middleware.go ProducerMiddleware type.
EnqueueFunc = Callable[..., Awaitable[str]]
ProducerMiddleware = Callable[[EnqueueFunc], EnqueueFunc]


def chain_consumer(handler: Handler, middlewares: list[ConsumerMiddleware]) -> Handler:
    """Compose consumer middlewares around a handler.

    Mirrors gomq/middleware.go chainConsumer() — applies in reverse order
    so the first middleware is the outermost wrapper.
    """
    for mw in reversed(middlewares):
        handler = mw(handler)
    return handler


def chain_producer(enqueue: EnqueueFunc, middlewares: list[ProducerMiddleware]) -> EnqueueFunc:
    """Compose producer middlewares around an enqueue function.

    Mirrors gomq/middleware.go chainProducer().
    """
    for mw in reversed(middlewares):
        enqueue = mw(enqueue)
    return enqueue
