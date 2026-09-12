"""The message record that flows from storage to publisher."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Any

#: Identifier of an outbox row. Opaque to the relay: it only passes ids back to storage.
MessageId = Any

_EMPTY_HEADERS: Mapping[str, str] = MappingProxyType({})


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
    headers: Mapping[str, str] = field(default=_EMPTY_HEADERS)
    #: How many times this message has been claimed, including the current claim.
    attempts: int = 1
    created_at: datetime | None = None
