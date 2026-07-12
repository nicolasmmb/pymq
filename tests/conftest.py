"""Shared test fixtures for pymq tests."""

import os
import uuid

import pytest_asyncio
from redis.asyncio import Redis

from pymq.queue import Queue


@pytest_asyncio.fixture
async def redis_client():
    """Create a Redis client connected to local Redis."""
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/15")
    client = Redis.from_url(url, decode_responses=False)
    await client.ping()
    yield client
    await client.aclose()


@pytest_asyncio.fixture
async def queue(redis_client):
    """Create a Queue with a unique name, flushed after each test."""
    name = f"test-{uuid.uuid4().hex[:8]}"
    q = Queue(redis_client, queue_name=name)
    yield q
    # Flush test keys
    keys = await redis_client.keys(f"gomq:{name}:*")
    if keys:
        await redis_client.delete(*keys)
