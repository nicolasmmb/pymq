"""Tests for pymq.errors module."""

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


class TestErrorCode:
    def test_enum_values(self):
        assert ErrorCode.DUPLICATE.value == "duplicate"
        assert ErrorCode.ALREADY_DONE.value == "already_done"
        assert ErrorCode.LOST_OWNERSHIP.value == "lost_ownership"
        assert ErrorCode.DLQ.value == "dlq"
        assert ErrorCode.INVALID_CONFIG.value == "invalid_config"
        assert ErrorCode.REDIS.value == "redis"
        assert ErrorCode.UNEXPECTED.value == "unexpected"


class TestGomqError:
    def test_format(self):
        err = GomqError(code=ErrorCode.DUPLICATE, op="enqueue", queue="q1", message_id="abc")
        msg = str(err)
        assert "pymq: duplicate" in msg
        assert "op=enqueue" in msg
        assert "queue=q1" in msg
        assert "message_id=abc" in msg

    def test_format_with_cause(self):
        cause = ValueError("bad")
        err = GomqError(code=ErrorCode.REDIS, cause=cause)
        assert "bad" in str(err)

    def test_is_subclass(self):
        assert issubclass(DuplicateError, GomqError)
        assert issubclass(PermanentError, GomqError)


class TestSentinels:
    def test_no_message(self):
        err = NoMessageError()
        assert isinstance(err, GomqError)

    def test_duplicate(self):
        err = DuplicateError()
        assert err.code == ErrorCode.DUPLICATE

    def test_already_done(self):
        err = AlreadyDoneError()
        assert err.code == ErrorCode.ALREADY_DONE

    def test_lost_ownership(self):
        err = LostOwnershipError()
        assert err.code == ErrorCode.LOST_OWNERSHIP

    def test_invalid_config(self):
        err = InvalidConfigError()
        assert err.code == ErrorCode.INVALID_CONFIG

    def test_dlq(self):
        err = DLQError()
        assert err.code == ErrorCode.DLQ


class TestPermanentError:
    def test_is_permanent(self):
        err = PermanentError(cause=ValueError("x"))
        assert is_permanent(err)

    def test_not_permanent(self):
        err = GomqError(code=ErrorCode.DUPLICATE)
        assert not is_permanent(err)

    def test_none_not_permanent(self):
        assert not is_permanent(None)
