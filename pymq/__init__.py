"""pymq — Redis-backed work queue for Python.

A Python port of gomq (https://github.com/nicolasmmb/gomq) providing
at-least-once delivery with Lua-atomic state transitions, visibility timeouts,
retries, dead-letter queue, fencing tokens, middlewares, hooks, and observability.
"""

from pymq.admin import MessageInfo, Stats
from pymq.errors import (
    AlreadyDoneError,
    DLQError,
    DuplicateError,
    ErrorCode,
    GomqError,
    InvalidConfigError,
    LostOwnershipError,
    NoMessageError,
    PermanentError,
    is_permanent,
)
from pymq.event import Event, EventStatus, Hooks, Observer, ObserverFunc, Operation
from pymq.keys import RedisKeys, make_redis_keys
from pymq.lifecycle import consume_with_signals, run_reaper
from pymq.managed import (
    MANAGED_WORKER_ALL,
    ManagedConsumerOptions,
    ManagedControl,
    ManagedWorkerInfo,
    ManagedWorkerState,
    list_managed_workers,
    pause_managed_workers,
    resume_managed_workers,
    run_managed_consumer,
    set_managed_concurrency,
)
from pymq.message import Delivery, Message
from pymq.middleware import (
    ConsumerMiddleware,
    Handler,
    ProducerMiddleware,
    chain_consumer,
    chain_producer,
)
from pymq.options import (
    AckRetentionPolicy,
    EnqueueOptions,
    ListOptions,
    NackOptions,
    Options,
    RequeueOptions,
)
from pymq.queue import Queue
from pymq.state import MessageState, can_transition

__all__ = [
    # Core
    "Queue",
    "Message",
    "Delivery",
    # Options
    "Options",
    "AckRetentionPolicy",
    "EnqueueOptions",
    "NackOptions",
    "ListOptions",
    "RequeueOptions",
    # Errors
    "GomqError",
    "PermanentError",
    "NoMessageError",
    "DuplicateError",
    "AlreadyDoneError",
    "LostOwnershipError",
    "InvalidConfigError",
    "DLQError",
    "ErrorCode",
    "is_permanent",
    # Events
    "Event",
    "EventStatus",
    "Operation",
    "Hooks",
    "Observer",
    "ObserverFunc",
    # State
    "MessageState",
    "can_transition",
    # Keys
    "RedisKeys",
    "make_redis_keys",
    # Admin
    "Stats",
    "MessageInfo",
    # Middleware
    "Handler",
    "ConsumerMiddleware",
    "ProducerMiddleware",
    "chain_consumer",
    "chain_producer",
    # Lifecycle
    "run_reaper",
    "consume_with_signals",
    # Managed
    "ManagedConsumerOptions",
    "ManagedWorkerInfo",
    "ManagedWorkerState",
    "ManagedControl",
    "MANAGED_WORKER_ALL",
    "run_managed_consumer",
    "list_managed_workers",
    "pause_managed_workers",
    "resume_managed_workers",
    "set_managed_concurrency",
]

__version__ = "0.1.0"
