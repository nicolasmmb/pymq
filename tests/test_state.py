"""Tests for pymq.state module."""

from pymq.state import MessageState, can_transition


class TestMessageState:
    def test_enum_values(self):
        assert MessageState.READY.value == "ready"
        assert MessageState.PROCESSING.value == "processing"
        assert MessageState.DONE.value == "done"
        assert MessageState.DLQ.value == "dlq"


class TestCanTransition:
    def test_empty_to_ready(self):
        assert can_transition("", "ready")

    def test_ready_to_processing(self):
        assert can_transition("ready", "processing")

    def test_processing_to_ready(self):
        assert can_transition("processing", "ready")

    def test_processing_to_done(self):
        assert can_transition("processing", "done")

    def test_processing_to_dlq(self):
        assert can_transition("processing", "dlq")

    def test_processing_to_processing(self):
        assert can_transition("processing", "processing")

    def test_done_to_any_fails(self):
        assert not can_transition("done", "ready")
        assert not can_transition("done", "processing")

    def test_dlq_to_any_fails(self):
        assert not can_transition("dlq", "ready")
        assert not can_transition("dlq", "processing")

    def test_ready_to_done_fails(self):
        assert not can_transition("ready", "done")

    def test_ready_to_dlq_fails(self):
        assert not can_transition("ready", "dlq")
