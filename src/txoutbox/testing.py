"""Contract tests for :class:`txoutbox.Storage` implementations.

Subclass :class:`StorageContract` in your test suite, provide a fresh ``storage``
fixture and implement :meth:`StorageContract.insert`::

    import pytest
    from txoutbox.testing import StorageContract

    class TestMyStorage(StorageContract):
        @pytest.fixture
        async def storage(self):
            return MyStorage(...)

        async def insert(self, storage, topic, payload, key=None):
            return await storage.add(topic, payload, key=key)

Every test claims with short leases, so keep the fixture cheap.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from .message import MessageId
from .protocols import Storage

LEASE = timedelta(seconds=30)
SHORT_LEASE = timedelta(milliseconds=50)


class StorageContract:
    """Behaviour every storage adapter must satisfy."""

    #: Set to ``False`` if your adapter does not implement strict per-key ordering.
    strict_ordering: bool = True

    async def insert(
        self, storage: Any, topic: str, payload: bytes, key: str | None = None
    ) -> MessageId:
        raise NotImplementedError("implement insert() for your storage")

    async def test_empty_claim(self, storage: Storage) -> None:
        assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")) == []

    async def test_claim_returns_in_insertion_order_with_attempt_one(
        self, storage: Storage
    ) -> None:
        ids = [await self.insert(storage, "t", f"p{i}".encode(), key="k") for i in range(3)]
        got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")
        assert [m.id for m in got] == ids
        assert [m.payload for m in got] == [b"p0", b"p1", b"p2"]
        assert all(m.attempts == 1 for m in got)
        assert all(m.topic == "t" and m.key == "k" for m in got)

    async def test_claim_respects_batch_size(self, storage: Storage) -> None:
        for i in range(5):
            await self.insert(storage, "t", f"p{i}".encode())
        first = await storage.claim(batch_size=2, lease=LEASE, worker_id="w1")
        second = await storage.claim(batch_size=2, lease=LEASE, worker_id="w1")
        third = await storage.claim(batch_size=2, lease=LEASE, worker_id="w1")
        assert len(first) == 2 and len(second) == 2 and len(third) == 1

    async def test_leased_rows_are_invisible_to_other_workers(self, storage: Storage) -> None:
        await self.insert(storage, "t", b"p")
        assert len(await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")) == 1
        assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w2")) == []

    async def test_expired_lease_is_reclaimable_and_counts_an_attempt(
        self, storage: Storage
    ) -> None:
        await self.insert(storage, "t", b"p")
        await storage.claim(batch_size=10, lease=SHORT_LEASE, worker_id="w1")
        await asyncio.sleep(SHORT_LEASE.total_seconds() * 2)
        got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w2")
        assert len(got) == 1 and got[0].attempts == 2

    async def test_acked_rows_are_never_claimed_again(self, storage: Storage) -> None:
        await self.insert(storage, "t", b"p")
        got = await storage.claim(batch_size=10, lease=SHORT_LEASE, worker_id="w1")
        await storage.ack([m.id for m in got])
        await asyncio.sleep(SHORT_LEASE.total_seconds() * 2)
        assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")) == []

    async def test_ack_empty_is_noop(self, storage: Storage) -> None:
        await storage.ack([])

    async def test_nack_schedules_retry(self, storage: Storage) -> None:
        await self.insert(storage, "t", b"p")
        (m,) = await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")
        await storage.nack(m.id, error="boom", retry_at=datetime.now(UTC) + timedelta(hours=1))
        assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")) == []

    async def test_nack_in_the_past_is_claimable_now(self, storage: Storage) -> None:
        await self.insert(storage, "t", b"p")
        (m,) = await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")
        await storage.nack(m.id, error="boom", retry_at=datetime.now(UTC) - timedelta(seconds=1))
        got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w2")
        assert len(got) == 1 and got[0].attempts == 2

    async def test_dead_letter_is_never_claimed_again(self, storage: Storage) -> None:
        await self.insert(storage, "t", b"p")
        (m,) = await storage.claim(batch_size=10, lease=SHORT_LEASE, worker_id="w1")
        await storage.dead_letter(m.id, error="gave up")
        await asyncio.sleep(SHORT_LEASE.total_seconds() * 2)
        assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")) == []

    async def test_concurrent_claims_do_not_overlap(self, storage: Storage) -> None:
        for i in range(20):
            await self.insert(storage, "t", f"p{i}".encode())
        results = await asyncio.gather(
            *(storage.claim(batch_size=5, lease=LEASE, worker_id=f"w{i}") for i in range(4))
        )
        ids = [m.id for batch in results for m in batch]
        assert len(ids) == 20 and len(set(ids)) == 20

    async def test_strict_ordering_holds_back_successors_of_a_retrying_message(
        self, storage: Storage
    ) -> None:
        if not self.strict_ordering:
            return
        first = await self.insert(storage, "t", b"1", key="k")
        await self.insert(storage, "t", b"2", key="k")
        other = await self.insert(storage, "t", b"x", key="other")
        (m1,) = await storage.claim(batch_size=1, lease=LEASE, worker_id="w1")
        assert m1.id == first
        await storage.nack(m1.id, error="boom", retry_at=datetime.now(UTC) + timedelta(hours=1))
        got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")
        assert [m.id for m in got] == [other]

    async def test_strict_ordering_holds_back_successors_of_a_leased_message(
        self, storage: Storage
    ) -> None:
        if not self.strict_ordering:
            return
        await self.insert(storage, "t", b"1", key="k")
        await self.insert(storage, "t", b"2", key="k")
        (m1,) = await storage.claim(batch_size=1, lease=LEASE, worker_id="w1")
        assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w2")) == []
        await storage.ack([m1.id])
        got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w2")
        assert [m.payload for m in got] == [b"2"]
