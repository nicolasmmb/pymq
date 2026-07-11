"""Admin operations — Stats, Inspect, ListDLQ, RequeueDLQ, DeleteDLQ.

Corresponds to gomq/admin.go — Stats, MessageInfo, ListOptions, parsers.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from pymq.errors import NoMessageError
from pymq.state import MessageState


@dataclass
class Stats:
    """Queue statistics.

    Corresponds to gomq/admin.go Stats struct.
    """

    ready: int = 0
    inflight: int = 0
    dlq: int = 0
    done: int = 0
    oldest_ready_age: timedelta = field(default_factory=timedelta)
    oldest_dlq_age: timedelta = field(default_factory=timedelta)


@dataclass
class MessageInfo:
    """Detailed info about a single message.

    Corresponds to gomq/admin.go MessageInfo struct.
    """

    id: str = ""
    payload: bytes = b""
    headers: dict[str, str] = field(default_factory=dict)
    state: MessageState = MessageState.READY
    attempts: int = 0
    lease_until_ms: int = 0
    last_error: str = ""
    dlq_score: float = 0.0
    done_score: float = 0.0


# Admin methods are mixed into Queue via the queue module.
# This module defines the data types and parsers only.


def parse_stats(result: Any) -> Stats:
    """Parse the Lua stats script result.

    Mirrors gomq/admin.go parseStats().
    """
    if not isinstance(result, (list, tuple)) or len(result) < 4:
        raise ValueError(f"unexpected stats result {result!r}")
    stats = Stats(
        ready=_to_int(result[0]),
        inflight=_to_int(result[1]),
        dlq=_to_int(result[2]),
        done=_to_int(result[3]),
    )
    if len(result) >= 6:
        stats.oldest_ready_age = timedelta(milliseconds=_to_int(result[4]))
        stats.oldest_dlq_age = timedelta(milliseconds=_to_int(result[5]))
    return stats


def parse_message_info(result: Any) -> MessageInfo:
    """Parse a single message info result from Lua.

    Mirrors gomq/admin.go parseMessageInfo().
    """
    if not isinstance(result, (list, tuple)):
        raise ValueError(f"unexpected message info result {result!r}")
    status = _to_str(result[0])
    if status != "ok":
        raise NoMessageError()
    if len(result) < 10:
        raise ValueError(f"unexpected message info result {result!r}")
    return _parse_message_info_fields(result[1:10])


def parse_message_info_list(result: Any) -> list[MessageInfo]:
    """Parse a DLQ list result from Lua.

    Mirrors gomq/admin.go parseMessageInfoList().
    """
    if not isinstance(result, (list, tuple)):
        raise ValueError(f"unexpected dlq list result {result!r}")
    out: list[MessageInfo] = []
    for row in result:
        if not isinstance(row, (list, tuple)) or len(row) < 8:
            raise ValueError(f"unexpected dlq row {row!r}")
        out.append(_parse_message_info_fields(row))
    return out


def _parse_message_info_fields(values: tuple | list) -> MessageInfo:
    """Parse message info fields from a Lua result array.

    Mirrors gomq/admin.go parseMessageInfoFields().
    """
    if len(values) < 8:
        raise ValueError(f"unexpected message fields {values!r}")
    info = MessageInfo(
        id=_to_str(values[0]),
        payload=(_to_str(values[1])).encode() if isinstance(values[1], str) else (values[1] or b""),
        state=MessageState(_to_str(values[3])) if values[3] else MessageState.READY,
        attempts=_to_int(values[4]),
        lease_until_ms=_to_int(values[5]),
        last_error=_to_str(values[6]),
        dlq_score=_to_float(values[7]),
    )
    if len(values) >= 9:
        info.done_score = _to_float(values[8])
    raw_headers = _to_str(values[2])
    if raw_headers:
        try:
            info.headers = json.loads(raw_headers)
        except (json.JSONDecodeError, TypeError):
            pass
    return info


def _to_int(v: Any) -> int:
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, bytes):
        return int(v)
    if isinstance(v, str):
        return int(v)
    return 0


def _to_float(v: Any) -> float:
    if isinstance(v, float):
        return v
    if isinstance(v, (int, bytes, str)):
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0


def _to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bytes):
        return v.decode()
    if isinstance(v, str):
        return v
    return str(v)
