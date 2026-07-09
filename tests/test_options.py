"""Tests for pymq.options module."""

import pytest
from datetime import timedelta

from pymq.errors import InvalidConfigError
from pymq.options import Options, default_backoff


class TestDefaultBackoff:
    def test_attempt_1(self):
        assert default_backoff(1) == timedelta(seconds=1)

    def test_attempt_3(self):
        assert default_backoff(3) == timedelta(seconds=3)

    def test_attempt_0_becomes_1(self):
        assert default_backoff(0) == timedelta(seconds=1)

    def test_cap_30s(self):
        assert default_backoff(100) == timedelta(seconds=30)


class TestOptions:
    def test_valid(self):
        opts = Options(queue_name="q1")
        assert opts.queue_name == "q1"
        assert opts.visibility_timeout == timedelta(seconds=30)
        assert opts.poll_timeout == timedelta(seconds=1)
        assert opts.poll_interval == timedelta(milliseconds=50)
        assert opts.heartbeat_interval == timedelta(seconds=10)
        assert opts.dedupe_ttl == timedelta(hours=24)
        assert opts.max_attempts == 3
        assert opts.consumer_id == "pymq-consumer"

    def test_empty_queue_name_fails(self):
        with pytest.raises(InvalidConfigError):
            Options(queue_name="")

    def test_zero_visibility_timeout_fails(self):
        with pytest.raises(InvalidConfigError):
            Options(queue_name="q1", visibility_timeout=timedelta(0))

    def test_negative_poll_timeout_fails(self):
        with pytest.raises(InvalidConfigError):
            Options(queue_name="q1", poll_timeout=timedelta(seconds=-1))

    def test_heartbeat_ge_visibility_fails(self):
        with pytest.raises(InvalidConfigError):
            Options(
                queue_name="q1",
                visibility_timeout=timedelta(seconds=10),
                heartbeat_interval=timedelta(seconds=10),
            )

    def test_poll_interval_gt_poll_timeout_fails(self):
        with pytest.raises(InvalidConfigError):
            Options(
                queue_name="q1",
                poll_timeout=timedelta(seconds=1),
                poll_interval=timedelta(seconds=2),
            )
