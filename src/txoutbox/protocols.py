"""The two protocols you implement, and the hooks you may implement."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timedelta
from typing import Protocol, TypeAlias, runtime_checkable

from .message import MessageId, OutboxMessage, OutboxStats


@runtime_checkable
class Storage(Protocol):
    """Where outbox rows live. Implement this over your table, your ORM, your driver.

    Contract (verified by :class:`txoutbox.testing.StorageContract`):

    * ``claim`` returns at most ``batch_size`` pending messages that are due and not
      leased, ordered by insertion, and leases them for ``lease`` on behalf of
      ``worker_id``. The ``attempts`` counter is incremented **at claim time**, so a
      worker that crashes mid-batch still burns an attempt.
    * Every other method is **fenced**: it only touches rows currently leased by
      ``worker_id``. A worker whose lease expired must not be able to affect a row
      that another worker has since claimed.
    * ``ack`` marks rows delivered. Whether you delete them or flip a status column is
      your business.
    * ``nack`` releases the lease, records ``error`` and schedules the next attempt at
      ``retry_at``.
    * ``release`` gives rows back **without** counting the attempt (``attempts - 1``) and
      without recording an error. The relay uses it for messages it never tried,
      because an earlier message with the same key failed.
    * ``dead_letter`` releases the lease and parks the row permanently.
    * Strict ordering (recommended, see the README): do not hand out a row while an
      earlier pending row with the same key exists that is not claimable right now.
    """

    async def claim(
        self, *, batch_size: int, lease: timedelta, worker_id: str
    ) -> Sequence[OutboxMessage]: ...

    async def ack(self, ids: Sequence[MessageId], *, worker_id: str) -> None: ...

    async def nack(
        self, message_id: MessageId, *, worker_id: str, error: str, retry_at: datetime
    ) -> None: ...

    async def release(
        self, ids: Sequence[MessageId], *, worker_id: str, retry_at: datetime
    ) -> None: ...

    async def dead_letter(self, message_id: MessageId, *, worker_id: str, error: str) -> None: ...


@runtime_checkable
class StatsProvider(Protocol):
    """Optional: storages that can report queue depth and lag. All built-in adapters do."""

    async def stats(self) -> OutboxStats: ...


@runtime_checkable
class Publisher(Protocol):
    """Where messages go. Raise on failure; the relay will retry."""

    async def publish(self, message: OutboxMessage) -> None: ...


#: A publisher may also be a plain ``async def publish(message)`` function.
PublishFn: TypeAlias = Callable[[OutboxMessage], Awaitable[None]]


class Hooks:
    """Observability callbacks. Subclass and override what you need; defaults are no-ops.

    Hooks must not raise. If they do, the exception is logged and swallowed so that
    telemetry can never break delivery.
    """

    async def on_claimed(self, messages: Sequence[OutboxMessage]) -> None:
        """A batch was claimed from storage. Called even for empty batches."""

    async def on_published(self, message: OutboxMessage) -> None:
        """The publisher accepted a message. The ack to storage follows at the end of the round."""

    async def on_retry(
        self, message: OutboxMessage, error: BaseException, retry_at: datetime
    ) -> None:
        """Publishing failed; the message was scheduled for another attempt."""

    async def on_dead_letter(self, message: OutboxMessage, error: BaseException) -> None:
        """Publishing failed for the last time; the message was parked."""

    async def on_blocked(self, message: OutboxMessage, blocked_by: OutboxMessage) -> None:
        """A message was released untried because an earlier one with the same key failed."""

    async def on_round(self, result: RoundResultLike, duration: float) -> None:
        """A round finished. ``duration`` is in seconds."""


class RoundResultLike(Protocol):
    claimed: int
    published: int
    retried: int
    dead_lettered: int
    blocked: int
