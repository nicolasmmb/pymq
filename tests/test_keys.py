"""Tests for pymq.keys module."""

from pymq.keys import RedisKeys, make_redis_keys


class TestMakeRedisKeys:
    def test_basic(self):
        keys = make_redis_keys("myqueue")
        assert keys.ready == "gomq:{myqueue}:ready"
        assert keys.inflight == "gomq:{myqueue}:inflight"
        assert keys.dlq == "gomq:{myqueue}:dlq"
        assert keys.done == "gomq:{myqueue}:done"
        assert keys.dedupe == "gomq:{myqueue}:dedupe"
        assert keys.payloads == "gomq:{myqueue}:payloads"
        assert keys.headers == "gomq:{myqueue}:headers"
        assert keys.states == "gomq:{myqueue}:states"
        assert keys.attempts == "gomq:{myqueue}:attempts"
        assert keys.tokens == "gomq:{myqueue}:tokens"
        assert keys.leases == "gomq:{myqueue}:leases"
        assert keys.errors == "gomq:{myqueue}:errors"
        assert keys.metadata == "gomq:{myqueue}:metadata"
        assert keys.workers == "gomq:{myqueue}:workers"
        assert keys.worker_info == "gomq:{myqueue}:worker_info"
        assert keys.worker_controls == "gomq:{myqueue}:worker_controls"

    def test_hash_tag_cluster_compatible(self):
        keys = make_redis_keys("q1")
        for field_name in RedisKeys.model_fields:
            val = getattr(keys, field_name)
            assert "{q1}" in val, f"Missing hash tag in {field_name}: {val}"
