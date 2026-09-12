"""A txoutbox Storage over the Django ORM (PostgreSQL).

Producer side, anywhere in your code::

    with transaction.atomic():
        order = Order.objects.create(total=42)
        OutboxRow.objects.create(
            topic="orders.created",
            payload=encode_payload({"order_id": order.id}),
            key=f"order-{order.id}",
        )

Relay side, e.g. in a management command::

    await Relay(DjangoStorage(), publish).run(handle_signals=True)

Strict per-key ordering is implemented the same way as ``txoutbox.adapters.postgres``:
a ``NOT EXISTS`` predicate plus a transaction-scoped advisory lock, so two relay
processes never split one key. ``select_for_update(skip_locked=True)`` needs
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta

from asgiref.sync import sync_to_async
from django.db import connection, transaction
from django.db.models import Exists, F, OuterRef, Q, QuerySet
from django.utils import timezone

from txoutbox import MessageId, OutboxMessage

from .models import OutboxRow

ADVISORY_LOCK_KEY = "txoutbox:outbox"


class DjangoStorage:
    def __init__(self, *, strict_ordering: bool = True) -> None:
        self.strict_ordering = strict_ordering

    @sync_to_async
    def claim(
        self, *, batch_size: int, lease: timedelta, worker_id: str
    ) -> Sequence[OutboxMessage]:
        now = timezone.now()
        with transaction.atomic():
            candidates = (
                OutboxRow.objects.filter(status="pending")
                .filter(Q(retry_at=None) | Q(retry_at__lte=now))
                .filter(Q(lease_until=None) | Q(lease_until__lte=now))
            )
            if self.strict_ordering:
                # Serialise claims across workers so the predicate sees committed state.
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT pg_advisory_xact_lock(hashtext(%s))", [ADVISORY_LOCK_KEY]
                    )
                blocked_by_earlier = OutboxRow.objects.filter(
                    key=OuterRef("key"), id__lt=OuterRef("id"), status="pending"
                ).filter(Q(retry_at__gt=now) | Q(lease_until__gt=now))
                candidates = candidates.filter(Q(key=None) | ~Exists(blocked_by_earlier))
            rows = list(candidates.select_for_update(skip_locked=True).order_by("id")[:batch_size])
            OutboxRow.objects.filter(id__in=[r.id for r in rows]).update(
                attempts=F("attempts") + 1,
                leased_by=worker_id,
                lease_until=now + lease,
                retry_at=None,
            )
        return [
            OutboxMessage(
                id=r.id,
                topic=r.topic,
                payload=bytes(r.payload),
                key=r.key,
                headers=r.headers,
                attempts=r.attempts + 1,
                created_at=r.created_at,
            )
            for r in rows
        ]

    @sync_to_async
    def ack(self, ids: Sequence[MessageId], *, worker_id: str) -> None:
        self._owned(ids, worker_id).update(status="done", leased_by=None, lease_until=None)

    @sync_to_async
    def nack(
        self, message_id: MessageId, *, worker_id: str, error: str, retry_at: datetime
    ) -> None:
        self._owned([message_id], worker_id).update(
            retry_at=retry_at, last_error=error, leased_by=None, lease_until=None
        )

    @sync_to_async
    def release(self, ids: Sequence[MessageId], *, worker_id: str, retry_at: datetime) -> None:
        self._owned(ids, worker_id).update(
            attempts=F("attempts") - 1, retry_at=retry_at, leased_by=None, lease_until=None
        )

    @sync_to_async
    def dead_letter(self, message_id: MessageId, *, worker_id: str, error: str) -> None:
        self._owned([message_id], worker_id).update(
            status="dead", last_error=error, leased_by=None, lease_until=None
        )

    @staticmethod
    def _owned(ids: Sequence[MessageId], worker_id: str) -> QuerySet[OutboxRow]:
        """Fencing: only rows this worker still holds."""
        return OutboxRow.objects.filter(id__in=list(ids), leased_by=worker_id)
