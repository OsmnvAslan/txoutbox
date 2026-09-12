"""The message record that flows from storage to publisher."""

from __future__ import annotations

import json
from collections.abc import Hashable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Any, TypeAlias

#: Identifier of an outbox row. Opaque to the relay: it only passes ids back to storage.
MessageId: TypeAlias = Hashable

#: What producers may pass as a payload. ``bytes`` go through untouched, ``str`` is
#: UTF-8 encoded, anything else is JSON-encoded (compact, UTF-8, non-ASCII preserved).
Payload: TypeAlias = bytes | str | Mapping[str, Any] | list[Any]

_EMPTY_HEADERS: Mapping[str, str] = MappingProxyType({})


def encode_payload(payload: Payload) -> bytes:
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        return payload.encode()
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()


@dataclass(frozen=True, slots=True, kw_only=True)
class OutboxMessage:
    """One row of the outbox table, as seen by the relay.

    Storage adapters build these from their rows; publishers receive them.
    Everything except ``id``, ``topic`` and ``payload`` is optional.
    """

    id: MessageId
    topic: str
    payload: bytes
    #: Ordering key. Messages sharing a key are published sequentially, in claim order.
    key: str | None = None
    headers: Mapping[str, str] = field(default_factory=lambda: _EMPTY_HEADERS)
    #: How many times this message has been claimed, including the current claim.
    attempts: int = 1
    created_at: datetime | None = None

    def json(self) -> Any:
        """Decode the payload as JSON."""
        return json.loads(self.payload)

    def text(self) -> str:
        """Decode the payload as UTF-8."""
        return self.payload.decode()


@dataclass(frozen=True, slots=True, kw_only=True)
class OutboxStats:
    """A snapshot for dashboards and alerts. See :class:`txoutbox.StatsProvider`."""

    pending: int
    #: Age of the oldest pending row, the number to alert on. ``None`` when nothing is pending.
    oldest_pending_age: timedelta | None
    dead: int = 0
