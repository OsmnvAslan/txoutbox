"""In-memory storage and publisher. Deterministic, dependency-free, test-friendly."""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from ..message import MessageId, OutboxMessage


class Status(StrEnum):
    PENDING = "pending"
    DONE = "done"
    DEAD = "dead"


@dataclass(slots=True)
class Record:
    message: OutboxMessage
    status: Status = Status.PENDING
    attempts: int = 0
    retry_at: datetime | None = None
    leased_by: str | None = None
    lease_until: datetime | None = None
    last_error: str | None = None


class MemoryStorage:
    """A dict-backed outbox. Rows are inserted with :meth:`add` (your "transaction").

    With ``strict_ordering`` (default), a message whose key has an earlier message that
    is pending but not claimable right now (leased elsewhere, or waiting for a retry)
    is held back, so retries never reorder a key.
    """

    def __init__(
        self, *, clock: Callable[[], datetime] | None = None, strict_ordering: bool = True
    ) -> None:
        self.strict_ordering = strict_ordering
        self._records: dict[MessageId, Record] = {}
        self._ids = itertools.count(1)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._lock = asyncio.Lock()

    # -- producer side -----------------------------------------------------------------

    def add(
        self,
        topic: str,
        payload: bytes,
        *,
        key: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> MessageId:
        message_id = next(self._ids)
        self._records[message_id] = Record(
            OutboxMessage(
                id=message_id,
                topic=topic,
                payload=payload,
                key=key,
                headers=dict(headers or {}),
                created_at=self._clock(),
            )
        )
        return message_id

    # -- inspection --------------------------------------------------------------------

    def get(self, message_id: MessageId) -> Record:
        return self._records[message_id]

    def records(self, status: Status | None = None) -> list[Record]:
        return [r for r in self._records.values() if status is None or r.status == status]

    def __len__(self) -> int:
        return len(self._records)

    # -- Storage protocol --------------------------------------------------------------

    async def claim(
        self, *, batch_size: int, lease: timedelta, worker_id: str
    ) -> Sequence[OutboxMessage]:
        async with self._lock:
            now = self._clock()
            claimed: list[OutboxMessage] = []
            blocked_keys: set[str] = set()
            for record in self._records.values():
                if len(claimed) >= batch_size:
                    break
                if record.status is not Status.PENDING:
                    continue
                key = record.message.key
                not_due = record.retry_at is not None and record.retry_at > now
                leased = record.lease_until is not None and record.lease_until > now
                if not_due or leased or (key is not None and key in blocked_keys):
                    if key is not None and self.strict_ordering:
                        blocked_keys.add(key)
                    continue
                record.attempts += 1
                record.leased_by = worker_id
                record.lease_until = now + lease
                record.retry_at = None
                claimed.append(_with_attempts(record.message, record.attempts))
            return claimed

    async def ack(self, ids: Sequence[MessageId]) -> None:
        async with self._lock:
            for message_id in ids:
                record = self._records[message_id]
                record.status = Status.DONE
                record.leased_by = record.lease_until = None

    async def nack(self, message_id: MessageId, *, error: str, retry_at: datetime) -> None:
        async with self._lock:
            record = self._records[message_id]
            record.last_error = error
            record.retry_at = retry_at
            record.leased_by = record.lease_until = None

    async def dead_letter(self, message_id: MessageId, *, error: str) -> None:
        async with self._lock:
            record = self._records[message_id]
            record.status = Status.DEAD
            record.last_error = error
            record.leased_by = record.lease_until = None


def _with_attempts(message: OutboxMessage, attempts: int) -> OutboxMessage:
    return OutboxMessage(
        id=message.id,
        topic=message.topic,
        payload=message.payload,
        key=message.key,
        headers=message.headers,
        attempts=attempts,
        created_at=message.created_at,
    )


@dataclass(slots=True)
class MemoryPublisher:
    """Collects published messages. ``fail`` decides which publishes raise."""

    published: list[OutboxMessage] = field(default_factory=list)
    fail: Callable[[OutboxMessage], BaseException | None] | None = None
    delay: float = 0.0

    async def publish(self, message: OutboxMessage) -> None:
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail is not None and (error := self.fail(message)) is not None:
            raise error
        self.published.append(message)

    @property
    def topics(self) -> list[str]:
        return [m.topic for m in self.published]
