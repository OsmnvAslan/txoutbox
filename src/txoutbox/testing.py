"""Contract tests for :class:`txoutbox.Storage` implementations.

Subclass :class:`StorageContract` in your test suite, provide a fresh ``storage``
fixture and implement :meth:`StorageContract.insert`. Requires ``pytest-asyncio`` in
``auto`` mode (``asyncio_mode = "auto"`` in your pytest config)::

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
import unittest
from datetime import UTC, datetime, timedelta
from typing import Any

from .message import MessageId
from .protocols import StatsProvider, Storage

LEASE = timedelta(seconds=30)
SHORT_LEASE = timedelta(milliseconds=50)
FUTURE = timedelta(hours=1)


async def _expire(lease: timedelta = SHORT_LEASE) -> None:
    await asyncio.sleep(lease.total_seconds() * 2)


class StorageContract:
    """Behaviour every storage adapter must satisfy."""

    #: Set to ``False`` if your adapter does not implement strict per-key ordering.
    strict_ordering: bool = True

    async def insert(
        self, storage: Any, topic: str, payload: bytes, key: str | None = None
    ) -> MessageId:
        raise NotImplementedError("implement insert() for your storage")

    # -- claim -------------------------------------------------------------------------

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
        assert all(m.created_at is not None for m in got)

    async def test_claim_respects_batch_size(self, storage: Storage) -> None:
        for i in range(5):
            await self.insert(storage, "t", f"p{i}".encode())
        sizes = [
            len(await storage.claim(batch_size=2, lease=LEASE, worker_id="w1")) for _ in range(3)
        ]
        assert sizes == [2, 2, 1]

    async def test_leased_rows_are_invisible_to_other_workers(self, storage: Storage) -> None:
        await self.insert(storage, "t", b"p")
        assert len(await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")) == 1
        assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w2")) == []

    async def test_expired_lease_is_reclaimable_and_counts_an_attempt(
        self, storage: Storage
    ) -> None:
        await self.insert(storage, "t", b"p")
        await storage.claim(batch_size=10, lease=SHORT_LEASE, worker_id="w1")
        await _expire()
        got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w2")
        assert len(got) == 1 and got[0].attempts == 2

    async def test_concurrent_claims_do_not_overlap(self, storage: Storage) -> None:
        for i in range(20):
            await self.insert(storage, "t", f"p{i}".encode())
        results = await asyncio.gather(
            *(storage.claim(batch_size=5, lease=LEASE, worker_id=f"w{i}") for i in range(4))
        )
        ids = [m.id for batch in results for m in batch]
        assert len(ids) == 20 and len(set(ids)) == 20

    # -- ack / nack / release / dead_letter --------------------------------------------

    async def test_acked_rows_are_never_claimed_again(self, storage: Storage) -> None:
        await self.insert(storage, "t", b"p")
        got = await storage.claim(batch_size=10, lease=SHORT_LEASE, worker_id="w1")
        await storage.ack([m.id for m in got], worker_id="w1")
        await _expire()
        assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")) == []

    async def test_ack_and_release_empty_are_noops(self, storage: Storage) -> None:
        await storage.ack([], worker_id="w1")
        await storage.release([], worker_id="w1", retry_at=datetime.now(UTC))

    async def test_nack_schedules_retry(self, storage: Storage) -> None:
        await self.insert(storage, "t", b"p")
        (m,) = await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")
        await storage.nack(m.id, worker_id="w1", error="boom", retry_at=_now() + FUTURE)
        assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")) == []

    async def test_nack_in_the_past_is_claimable_now_and_counts(self, storage: Storage) -> None:
        await self.insert(storage, "t", b"p")
        (m,) = await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")
        await storage.nack(m.id, worker_id="w1", error="boom", retry_at=_now() - FUTURE)
        got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w2")
        assert len(got) == 1 and got[0].attempts == 2

    async def test_release_gives_back_without_burning_an_attempt(self, storage: Storage) -> None:
        a = await self.insert(storage, "t", b"a")
        b = await self.insert(storage, "t", b"b")
        got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")
        assert [m.attempts for m in got] == [1, 1]
        await storage.release([a, b], worker_id="w1", retry_at=_now() + FUTURE)
        assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w2")) == []

    async def test_release_in_the_past_is_claimable_with_same_attempt(
        self, storage: Storage
    ) -> None:
        a = await self.insert(storage, "t", b"a")
        await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")
        await storage.release([a], worker_id="w1", retry_at=_now() - FUTURE)
        got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w2")
        assert len(got) == 1 and got[0].attempts == 1

    async def test_dead_letter_is_never_claimed_again(self, storage: Storage) -> None:
        await self.insert(storage, "t", b"p")
        (m,) = await storage.claim(batch_size=10, lease=SHORT_LEASE, worker_id="w1")
        await storage.dead_letter(m.id, worker_id="w1", error="gave up")
        await _expire()
        assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")) == []

    # -- fencing -----------------------------------------------------------------------

    async def test_stale_ack_does_not_touch_a_row_leased_by_another_worker(
        self, storage: Storage
    ) -> None:
        a = await self.insert(storage, "t", b"a")
        await storage.claim(batch_size=10, lease=SHORT_LEASE, worker_id="w1")
        await _expire()
        (m,) = await storage.claim(batch_size=10, lease=LEASE, worker_id="w2")
        await storage.ack([a], worker_id="w1")  # w1 is late; must be ignored
        assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w3")) == []
        await storage.nack(m.id, worker_id="w2", error="x", retry_at=_now() - FUTURE)
        got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w3")
        assert [x.id for x in got] == [a]  # still pending, not acked by the stale worker

    async def test_stale_nack_does_not_break_another_workers_lease(self, storage: Storage) -> None:
        a = await self.insert(storage, "t", b"a")
        await storage.claim(batch_size=10, lease=SHORT_LEASE, worker_id="w1")
        await _expire()
        await storage.claim(batch_size=10, lease=LEASE, worker_id="w2")
        await storage.nack(a, worker_id="w1", error="late", retry_at=_now() - FUTURE)
        await storage.release([a], worker_id="w1", retry_at=_now() - FUTURE)
        assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w3")) == []

    async def test_stale_dead_letter_is_ignored(self, storage: Storage) -> None:
        a = await self.insert(storage, "t", b"a")
        await storage.claim(batch_size=10, lease=SHORT_LEASE, worker_id="w1")
        await _expire()
        await storage.claim(batch_size=10, lease=LEASE, worker_id="w2")
        await storage.dead_letter(a, worker_id="w1", error="late")
        await storage.nack(a, worker_id="w2", error="x", retry_at=_now() - FUTURE)
        assert len(await storage.claim(batch_size=10, lease=LEASE, worker_id="w3")) == 1

    # -- strict ordering ---------------------------------------------------------------

    async def test_strict_ordering_holds_back_successors_of_a_retrying_message(
        self, storage: Storage
    ) -> None:
        self._require_strict()
        first = await self.insert(storage, "t", b"1", key="k")
        await self.insert(storage, "t", b"2", key="k")
        other = await self.insert(storage, "t", b"x", key="other")
        (m1,) = await storage.claim(batch_size=1, lease=LEASE, worker_id="w1")
        assert m1.id == first
        await storage.nack(m1.id, worker_id="w1", error="boom", retry_at=_now() + FUTURE)
        got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w1")
        assert [m.id for m in got] == [other]

    async def test_strict_ordering_holds_back_successors_of_a_leased_message(
        self, storage: Storage
    ) -> None:
        self._require_strict()
        await self.insert(storage, "t", b"1", key="k")
        await self.insert(storage, "t", b"2", key="k")
        (m1,) = await storage.claim(batch_size=1, lease=LEASE, worker_id="w1")
        assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w2")) == []
        await storage.ack([m1.id], worker_id="w1")
        got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w2")
        assert [m.payload for m in got] == [b"2"]

    async def test_strict_ordering_under_concurrent_claims(self, storage: Storage) -> None:
        """Two workers racing for the same key must never split it."""
        self._require_strict()
        for i in range(6):
            await self.insert(storage, "t", str(i).encode(), key="k")
        for _ in range(3):
            results = await asyncio.gather(
                storage.claim(batch_size=1, lease=LEASE, worker_id="a"),
                storage.claim(batch_size=1, lease=LEASE, worker_id="b"),
            )
            claimed = [m for batch in results for m in batch]
            assert len(claimed) == 1, [m.payload for m in claimed]
            worker = "a" if results[0] else "b"
            await storage.ack([claimed[0].id], worker_id=worker)

    # -- stats -------------------------------------------------------------------------

    async def test_stats(self, storage: Storage) -> None:
        if not isinstance(storage, StatsProvider):
            raise unittest.SkipTest("storage does not provide stats()")
        empty = await storage.stats()
        assert (empty.pending, empty.oldest_pending_age, empty.dead) == (0, None, 0)
        await self.insert(storage, "t", b"a")
        await asyncio.sleep(0.01)
        await self.insert(storage, "t", b"b")
        s = await storage.stats()
        assert s.pending == 2 and s.oldest_pending_age is not None
        assert s.oldest_pending_age >= timedelta(milliseconds=10)
        (m, _) = await storage.claim(batch_size=2, lease=LEASE, worker_id="w1")
        await storage.dead_letter(m.id, worker_id="w1", error="x")
        s = await storage.stats()
        assert (s.pending, s.dead) == (1, 1)

    # -- helpers -----------------------------------------------------------------------

    def _require_strict(self) -> None:
        if not self.strict_ordering:
            raise unittest.SkipTest("adapter does not implement strict ordering")


def _now() -> datetime:
    return datetime.now(UTC)
