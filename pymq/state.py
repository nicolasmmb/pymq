"""Message state machine.

Corresponds to gomq/state.go — MessageState enum and canTransition().
"""

from __future__ import annotations

from enum import Enum


class MessageState(str, Enum):
    READY = "ready"
    PROCESSING = "processing"
    DONE = "done"
    DLQ = "dlq"


def can_transition(from_state: MessageState | str, to_state: MessageState | str) -> bool:
    """Check if a state transition is valid.

    State machine (from gomq/state.go):
        "" -> ready
        ready -> processing
        processing -> ready | done | dlq | processing
        done -> (none)
        dlq -> (none)
    """
    # Handle empty string as a special "unset" state (before first enqueue)
    f = from_state if isinstance(from_state, MessageState) else MessageState(from_state) if from_state else ""
    t = to_state if isinstance(to_state, MessageState) else MessageState(to_state)

    if f == "":
        return t == MessageState.READY
    if f == MessageState.READY:
        return t == MessageState.PROCESSING
    if f == MessageState.PROCESSING:
        return t in (MessageState.READY, MessageState.DONE, MessageState.DLQ, MessageState.PROCESSING)
    if f in (MessageState.DONE, MessageState.DLQ):
        return False
    return False
