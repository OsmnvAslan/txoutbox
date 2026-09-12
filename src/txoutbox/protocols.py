"""The two protocols you implement, and the hooks you may implement."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Protocol, runtime_checkable

from .message import MessageId, OutboxMessage


@runtime_checkable
class Storage(Protocol):
    """Where outbox rows live. Implement this over your table, your ORM, your driver.

    Contract (verified by :class:`txoutbox.testing.StorageContract`):

    * ``claim`` returns at most ``batch_size`` pending messages whose ``retry_at`` is
      due and which are not leased by another worker, ordered by insertion, and
      leases them for ``lease`` on behalf of ``worker_id``. The ``attempts`` counter is
      incremented **at claim time**, so a worker that crashes mid-batch still burns
      an attempt.
    * ``ack`` marks the messages as delivered. Whether you delete them or flip a
      status column is your business.
    * ``nack`` releases the lease and schedules the next attempt at ``retry_at``.
    * ``dead_letter`` releases the lease and parks the message permanently.
    """

    async def claim(
        self, *, batch_size: int, lease: timedelta, worker_id: str
    ) -> Sequence[OutboxMessage]: ...

    async def ack(self, ids: Sequence[MessageId]) -> None: ...

    async def nack(self, message_id: MessageId, *, error: str, retry_at: datetime) -> None: ...

    async def dead_letter(self, message_id: MessageId, *, error: str) -> None: ...


@runtime_checkable
class Publisher(Protocol):
    """Where messages go. Raise on failure; the relay will retry."""

    async def publish(self, message: OutboxMessage) -> None: ...


class Hooks:
    """Observability callbacks. Subclass and override what you need; defaults are no-ops.

    Hooks must not raise. If they do, the exception is logged and swallowed so that
    telemetry can never break delivery.
    """

    async def on_claimed(self, messages: Sequence[OutboxMessage]) -> None:
        """A batch was claimed from storage. Called even for empty batches."""

    async def on_published(self, message: OutboxMessage) -> None:
        """A message was published and acked."""

    async def on_retry(
        self, message: OutboxMessage, error: BaseException, retry_at: datetime
    ) -> None:
        """Publishing failed; the message was scheduled for another attempt."""

    async def on_dead_letter(self, message: OutboxMessage, error: BaseException) -> None:
        """Publishing failed for the last time; the message was parked."""

    async def on_blocked(self, message: OutboxMessage, blocked_by: OutboxMessage) -> None:
        """A message was skipped this round because an earlier one with the same key failed."""
