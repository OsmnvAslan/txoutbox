"""Contract tests for PostgresStorage.

Uses ``TXOUTBOX_PG_DSN`` when set; otherwise starts an embedded PostgreSQL through
the ``pgserver`` package. Skips when neither is available.
"""

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

import pytest

asyncpg = pytest.importorskip("asyncpg")

from txoutbox.adapters.postgres import PostgresStorage  # noqa: E402
from txoutbox.testing import StorageContract  # noqa: E402

LEASE = timedelta(seconds=30)


@pytest.fixture(scope="session")
def dsn(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    if env := os.environ.get("TXOUTBOX_PG_DSN"):
        yield env
        return
    try:
        import pgserver
    except ImportError:
        pytest.skip("set TXOUTBOX_PG_DSN or install pgserver to run Postgres tests")
    pgdata = tmp_path_factory.mktemp("pgdata")
    try:
        server = pgserver.get_server(str(pgdata))
    except Exception as exc:
        pytest.skip(f"embedded Postgres unavailable: {exc}")
    try:
        yield server.get_uri()
    finally:
        server.cleanup()


@pytest.fixture
async def pool(dsn: str) -> AsyncIterator["asyncpg.Pool"]:
    # Function-scoped: pytest-asyncio runs each test on a fresh loop.
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=6)
    yield pool
    await pool.close()


def _table() -> str:
    return f"outbox_{uuid.uuid4().hex[:8]}"


async def _make(pool: Any, **kwargs: Any) -> PostgresStorage:
    storage = PostgresStorage(pool, table=_table(), **kwargs)
    await storage.create_schema()
    return storage


async def _drop(pool: Any, storage: PostgresStorage) -> None:
    await pool.execute(f"DROP TABLE {storage.qualified_table} CASCADE")


class _PostgresContract(StorageContract):
    kwargs: ClassVar[dict[str, Any]] = {}

    @pytest.fixture
    async def storage(self, pool: "asyncpg.Pool") -> AsyncIterator[PostgresStorage]:
        storage = await _make(pool, **self.kwargs)
        yield storage
        await _drop(pool, storage)

    async def insert(self, storage, topic, payload, key=None):
        return await storage.add(topic, payload, key=key)


class TestPostgresStorage(_PostgresContract):
    pass


class TestPostgresStorageDeleteOnAck(_PostgresContract):
    kwargs: ClassVar[dict[str, Any]] = {"delete_on_ack": True}

    async def test_ack_deletes_rows(self, storage: PostgresStorage, pool) -> None:
        await storage.add("t", b"p")
        got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w")
        await storage.ack([m.id for m in got], worker_id="w")
        assert await pool.fetchval(f"SELECT count(*) FROM {storage.qualified_table}") == 0

    async def test_purge_is_noop(self, storage: PostgresStorage) -> None:
        assert await storage.purge(timedelta(0)) == 0


class TestPostgresStorageLoose(_PostgresContract):
    strict_ordering = False
    kwargs: ClassVar[dict[str, Any]] = {"strict_ordering": False}


# -- producer side ---------------------------------------------------------------------


async def test_insert_inside_user_transaction(pool) -> None:
    storage = await _make(pool)
    try:
        async with pool.acquire() as conn, conn.transaction():
            await storage.insert(conn, "orders.created", b"rolled back", key="o1")
            raise _Abort
    except _Abort:
        pass
    async with pool.acquire() as conn, conn.transaction():
        row_id = await storage.insert(
            conn, "orders.created", b'{"total": 42}', key="o2", headers={"v": "1"}
        )
    got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w")
    assert [m.id for m in got] == [row_id]
    assert got[0].payload == b'{"total": 42}'
    assert got[0].headers == {"v": "1"}
    assert got[0].key == "o2"
    assert got[0].created_at is not None and got[0].created_at.tzinfo is not None
    await _drop(pool, storage)


class _Abort(Exception):
    pass


async def test_payload_encoding(pool) -> None:
    storage = await _make(pool)
    await storage.add("t", {"order_id": 7, "city": "Köln"}, key="o7")
    await storage.add("t", "plain text")
    await storage.add("t", b"\x00raw")
    got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w")
    assert got[0].json() == {"order_id": 7, "city": "Köln"}
    assert got[0].payload == '{"order_id":7,"city":"Köln"}'.encode()
    assert got[1].text() == "plain text"
    assert got[2].payload == b"\x00raw"
    await _drop(pool, storage)


# -- fencing / release at the SQL level ------------------------------------------------


async def test_release_decrements_attempts_and_keeps_last_error(pool) -> None:
    storage = await _make(pool)
    row_id = await storage.add("t", b"p")
    await storage.claim(batch_size=1, lease=LEASE, worker_id="w")
    await storage.nack(row_id, worker_id="w", error="first", retry_at=datetime.now(UTC))
    (m,) = await storage.claim(batch_size=1, lease=LEASE, worker_id="w")
    assert m.attempts == 2
    await storage.release([row_id], worker_id="w", retry_at=datetime.now(UTC))
    row = await pool.fetchrow(
        f"SELECT attempts, last_error, leased_by FROM {storage.qualified_table} WHERE id = $1",
        row_id,
    )
    assert (row["attempts"], row["last_error"], row["leased_by"]) == (1, "first", None)
    await _drop(pool, storage)


# -- strict ordering under concurrency -------------------------------------------------


@pytest.mark.parametrize("attempt", range(10))
async def test_concurrent_workers_never_split_a_key(pool, attempt: int) -> None:
    storage = await _make(pool)
    for i in range(4):
        await storage.add("t", str(i).encode(), key="k")
    for _ in range(4):
        results = await asyncio.gather(
            *(storage.claim(batch_size=2, lease=LEASE, worker_id=f"w{i}") for i in range(4))
        )
        winners = [(f"w{i}", batch) for i, batch in enumerate(results) if batch]
        assert len(winners) == 1, [[m.payload for m in b] for _, b in winners]
        worker, batch = winners[0]
        await storage.ack([batch[0].id], worker_id=worker)
        await storage.release(
            [m.id for m in batch[1:]], worker_id=worker, retry_at=datetime.now(UTC)
        )
    await _drop(pool, storage)


async def test_claim_held_in_open_transaction_blocks_other_worker(pool) -> None:
    """Regression for 0.1.0: worker A's uncommitted claim was invisible to worker B's
    NOT EXISTS predicate, and SKIP LOCKED let B take the next message of the same key."""
    storage = await _make(pool)
    await storage.add("t", b"1", key="k")
    await storage.add("t", b"2", key="k")
    sql = storage._sql

    async def worker_a() -> list[bytes]:
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute(sql.lock, sql.lock_key)
            rows = await conn.fetch(sql.claim, "a", LEASE, 1)
            await asyncio.sleep(0.2)  # hold the transaction open while B claims
            return [bytes(r["payload"]) for r in rows]

    async def worker_b() -> list[bytes]:
        await asyncio.sleep(0.05)  # make sure A is inside its transaction
        got = await storage.claim(batch_size=10, lease=LEASE, worker_id="b")
        return [m.payload for m in got]

    a, b = await asyncio.gather(worker_a(), worker_b())
    assert a == [b"1"]
    assert b == []
    await _drop(pool, storage)


async def test_loose_ordering_skips_the_advisory_lock(pool) -> None:
    storage = await _make(pool, strict_ordering=False)
    await storage.add("t", b"1", key="k")
    await storage.add("t", b"2", key="k")

    async def hold_lock() -> None:
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute(storage._sql.lock, storage._sql.lock_key)
            await asyncio.sleep(0.3)

    async def claim() -> int:
        await asyncio.sleep(0.05)
        async with asyncio.timeout(0.2):  # would block on the lock if it were taken
            return len(await storage.claim(batch_size=10, lease=LEASE, worker_id="w"))

    _, n = await asyncio.gather(hold_lock(), claim())
    assert n == 2
    await _drop(pool, storage)


# -- visibility delay ------------------------------------------------------------------


async def test_visibility_delay(pool) -> None:
    storage = await _make(pool, visibility_delay=timedelta(milliseconds=300))
    await storage.add("t", b"fresh")
    assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w")) == []
    await asyncio.sleep(0.4)
    got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w")
    assert [m.payload for m in got] == [b"fresh"]
    await _drop(pool, storage)


def test_visibility_delay_validation() -> None:
    with pytest.raises(ValueError):
        PostgresStorage(object(), visibility_delay=timedelta(seconds=-1))  # type: ignore[arg-type]


# -- operations ------------------------------------------------------------------------


async def test_purge_deletes_old_done_rows_only(pool) -> None:
    storage = await _make(pool)
    ids = [await storage.add("t", str(i).encode()) for i in range(3)]
    got = await storage.claim(batch_size=2, lease=LEASE, worker_id="w")
    await storage.ack([m.id for m in got], worker_id="w")
    await pool.execute(
        f"UPDATE {storage.qualified_table} SET created_at = now() - interval '2 days'"
        " WHERE id = $1",
        ids[0],
    )
    assert await storage.purge(timedelta(days=1)) == 1
    remaining = await pool.fetch(f"SELECT id FROM {storage.qualified_table} ORDER BY id")
    assert [r["id"] for r in remaining] == ids[1:]
    await _drop(pool, storage)


async def test_stats_ages_are_timedeltas(pool) -> None:
    storage = await _make(pool)
    await storage.add("t", b"a")
    s = await storage.stats()
    assert isinstance(s.oldest_pending_age, timedelta) and s.pending == 1
    await _drop(pool, storage)


async def test_notify_and_listen(pool) -> None:
    channel = f"ch_{uuid.uuid4().hex[:8]}"
    storage = await _make(pool, notify_channel=channel)
    assert "pg_notify" in storage.schema_sql()
    fired = asyncio.Event()
    loop = asyncio.get_running_loop()
    listener = await storage.listen(lambda: loop.call_soon_threadsafe(fired.set))
    try:
        await storage.add("t", b"p")
        async with asyncio.timeout(1):
            await fired.wait()
    finally:
        await listener.close()
    # the connection went back to the pool and the pool is still fully usable
    assert await pool.fetchval("SELECT 1") == 1
    await _drop(pool, storage)


async def test_listen_defers_wake_by_visibility_delay(pool) -> None:
    channel = f"ch_{uuid.uuid4().hex[:8]}"
    storage = await _make(
        pool, notify_channel=channel, visibility_delay=timedelta(milliseconds=300)
    )
    fired: list[float] = []
    loop = asyncio.get_running_loop()
    listener = await storage.listen(lambda: fired.append(loop.time()))
    try:
        inserted = loop.time()
        await storage.add("t", b"p")
        await storage.add("t", b"q")  # second NOTIFY while the timer is pending: no extra wake
        await asyncio.sleep(0.1)
        assert fired == [], "callback must wait for the row to become visible"
        await asyncio.sleep(0.5)
        assert len(fired) == 1
        assert fired[0] - inserted >= 0.3
        # the deferred wake lands on a claimable row
        got = await storage.claim(batch_size=10, lease=LEASE, worker_id="w")
        assert len(got) == 2
    finally:
        await listener.close()
    await _drop(pool, storage)


async def test_listener_reconnects_after_backend_termination(pool) -> None:
    channel = f"ch_{uuid.uuid4().hex[:8]}"
    storage = await _make(pool, notify_channel=channel)
    hits = 0

    def bump() -> None:
        nonlocal hits
        hits += 1

    listener = await storage.listen(bump)
    try:
        assert listener.connection is not None
        pid = listener.connection.get_server_pid()
        assert await pool.fetchval("SELECT pg_terminate_backend($1)", pid) is True
        # wait for the listener to notice and come back on a fresh connection
        async with asyncio.timeout(3):
            while listener.connection is None or listener.connection.get_server_pid() == pid:
                await asyncio.sleep(0.05)
        await storage.add("t", b"after")
        async with asyncio.timeout(3):
            while hits == 0:
                await asyncio.sleep(0.05)
        assert hits == 1
    finally:
        await listener.close()
    assert await pool.fetchval("SELECT 1") == 1
    await _drop(pool, storage)


async def test_listener_stops_reconnecting_when_closed_during_gap(pool) -> None:
    channel = f"ch_{uuid.uuid4().hex[:8]}"
    storage = await _make(pool, notify_channel=channel)
    listener = await storage.listen(lambda: None)
    assert listener.connection is not None
    pid = listener.connection.get_server_pid()
    await pool.fetchval("SELECT pg_terminate_backend($1)", pid)
    await asyncio.sleep(0.05)
    await listener.close()  # must not hang or raise while a reconnect is in flight
    assert listener.connection is None
    assert await pool.fetchval("SELECT 1") == 1
    await _drop(pool, storage)


async def test_stats_uses_two_index_friendly_queries(pool) -> None:
    storage = await _make(pool)
    assert "FILTER" not in storage._sql.stats_pending
    assert "WHERE status = 'pending'" in storage._sql.stats_pending
    assert "WHERE status = 'dead'" in storage._sql.stats_dead
    await storage.add("t", b"a")
    (m,) = await storage.claim(batch_size=1, lease=LEASE, worker_id="w")
    await storage.dead_letter(m.id, worker_id="w", error="x")
    await storage.add("t", b"b")
    s = await storage.stats()
    assert (s.pending, s.dead) == (1, 1) and s.oldest_pending_age is not None
    await _drop(pool, storage)


async def test_listen_requires_channel(pool) -> None:
    storage = PostgresStorage(pool)
    with pytest.raises(ValueError):
        await storage.listen(lambda: None)


async def test_listener_context_manager(pool) -> None:
    channel = f"ch_{uuid.uuid4().hex[:8]}"
    storage = await _make(pool, notify_channel=channel)
    hits = 0

    def bump() -> None:
        nonlocal hits
        hits += 1

    async with await storage.listen(bump):
        await storage.add("t", b"p")
        await asyncio.sleep(0.2)
    assert hits == 1
    await storage.add("t", b"q")  # listener closed: no more callbacks
    await asyncio.sleep(0.2)
    assert hits == 1
    await _drop(pool, storage)


# -- misc ------------------------------------------------------------------------------


async def test_connect_owns_and_closes_pool(dsn: str) -> None:
    storage = await PostgresStorage.connect(dsn, table=_table(), min_size=1, max_size=2)
    await storage.create_schema()
    await storage.add("t", b"p")
    assert len(await storage.claim(batch_size=1, lease=LEASE, worker_id="w")) == 1
    await storage.pool.execute(f"DROP TABLE {storage.qualified_table}")
    await storage.close()
    assert storage.pool.is_closing()


async def test_schema_argument_and_sql(pool) -> None:
    schema = f"s_{uuid.uuid4().hex[:8]}"
    await pool.execute(f"CREATE SCHEMA {schema}")
    storage = PostgresStorage(pool, table="outbox", schema=schema, notify_channel="outbox_ch")
    assert storage.qualified_table == f"{schema}.outbox"
    sql = storage.schema_sql()
    assert f"CREATE TABLE IF NOT EXISTS {schema}.outbox" in sql
    assert "outbox_key_pending_idx" in sql
    assert f"FUNCTION {schema}.outbox_notify()" in sql
    await storage.create_schema()
    await storage.add("t", b"p")
    assert len(await storage.claim(batch_size=5, lease=LEASE, worker_id="w")) == 1
    await pool.execute(f"DROP SCHEMA {schema} CASCADE")


def test_identifier_validation() -> None:
    with pytest.raises(ValueError):
        PostgresStorage(object(), table="outbox; drop table users")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        PostgresStorage(object(), schema="public; --")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        PostgresStorage(object(), notify_channel="a b")  # type: ignore[arg-type]
