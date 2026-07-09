"""Redis key generation for a queue.

Corresponds to gomq/keys.go — redisKeys struct and makeRedisKeys().
All keys use hash tags gomq:{queue_name}:* for Redis Cluster compatibility.
"""

from __future__ import annotations

from pydantic import BaseModel


class RedisKeys(BaseModel):
    """All Redis key names for a single queue."""

    ready: str
    inflight: str
    dlq: str
    done: str
    payloads: str
    headers: str
    states: str
    attempts: str
    tokens: str
    leases: str
    errors: str
    metadata: str
    workers: str
    worker_info: str
    worker_controls: str


def make_redis_keys(queue_name: str) -> RedisKeys:
    """Build all Redis key names for a queue.

    Mirrors gomq/keys.go makeRedisKeys().
    Uses hash tags {queue_name} for Redis Cluster slot compatibility.
    """
    tag = f"gomq:{{{queue_name}}}"
    return RedisKeys(
        ready=f"{tag}:ready",
        inflight=f"{tag}:inflight",
        dlq=f"{tag}:dlq",
        done=f"{tag}:done",
        payloads=f"{tag}:payloads",
        headers=f"{tag}:headers",
        states=f"{tag}:states",
        attempts=f"{tag}:attempts",
        tokens=f"{tag}:tokens",
        leases=f"{tag}:leases",
        errors=f"{tag}:errors",
        metadata=f"{tag}:metadata",
        workers=f"{tag}:workers",
        worker_info=f"{tag}:worker_info",
        worker_controls=f"{tag}:worker_controls",
    )
