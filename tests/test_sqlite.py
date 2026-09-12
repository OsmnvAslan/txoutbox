import sqlite3
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import pytest

from txoutbox.adapters.sqlite import SqliteStorage
from txoutbox.testing import StorageContract

LEASE = timedelta(seconds=1)


async def _open(tmp_path: Path, **kw: object) -> AsyncIterator[SqliteStorage]:
    s = SqliteStorage(str(tmp_path / "outbox.db"), **kw)  # type: ignore[arg-type]
    s.create_schema()
    yield s
    s.close()


class TestSqliteStorage(StorageContract):
    @pytest.fixture
    async def storage(self, tmp_path: Path) -> AsyncIterator[SqliteStorage]:
        async for s in _open(tmp_path):
            yield s

    async def insert(self, storage, topic, payload, key=None):
        return await storage.add(topic, payload, key=key)


class TestSqliteStorageDeleteOnAck(TestSqliteStorage):
    @pytest.fixture
    async def storage(self, tmp_path: Path) -> AsyncIterator[SqliteStorage]:
        async for s in _open(tmp_path, delete_on_ack=True):
            yield s


class TestSqliteStorageLoose(TestSqliteStorage):
    strict_ordering = False

    @pytest.fixture
    async def storage(self, tmp_path: Path) -> AsyncIterator[SqliteStorage]:
        async for s in _open(tmp_path, strict_ordering=False):
            yield s


def test_rejects_bad_table_name(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        SqliteStorage(str(tmp_path / "x.db"), table="outbox; drop table users")


def test_schema_sql_names_the_table(tmp_path: Path) -> None:
    s = SqliteStorage(str(tmp_path / "x.db"), table="events_outbox")
    assert "CREATE TABLE IF NOT EXISTS events_outbox" in s.schema_sql()
    s.close()


async def test_insert_inside_user_transaction_with_json_payload(tmp_path: Path) -> None:
    path = str(tmp_path / "app.db")
    storage = SqliteStorage(path)
    storage.create_schema()
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")
    with conn:  # one transaction: business row + outbox row
        cur = conn.execute("INSERT INTO orders (total) VALUES (42)")
        storage.insert(
            conn,
            "orders.created",
            {"order_id": cur.lastrowid, "total": 42, "note": "тест"},
            key=f"order-{cur.lastrowid}",
            headers={"v": "1"},
        )
    (m,) = await storage.claim(batch_size=10, lease=LEASE, worker_id="w")
    assert m.json() == {"order_id": 1, "total": 42, "note": "тест"}
    assert m.headers == {"v": "1"} and m.key == "order-1" and m.created_at is not None
    conn.close()
    storage.close()


async def test_insert_rolled_back_with_user_transaction(tmp_path: Path) -> None:
    path = str(tmp_path / "app.db")
    storage = SqliteStorage(path)
    storage.create_schema()
    conn = sqlite3.connect(path)
    try:
        with conn:
            storage.insert(conn, "t", "never")
            raise RuntimeError("business logic failed")
    except RuntimeError:
        pass
    assert list(await storage.claim(batch_size=10, lease=LEASE, worker_id="w")) == []
    conn.close()
    storage.close()


async def test_purge_removes_old_done_rows_only(tmp_path: Path) -> None:
    storage = SqliteStorage(str(tmp_path / "x.db"))
    storage.create_schema()
    done = await storage.add("t", b"done")
    await storage.add("t", b"pending")
    await storage.claim(batch_size=1, lease=LEASE, worker_id="w")
    await storage.ack([done], worker_id="w")
    assert await storage.purge(older_than=timedelta(hours=1)) == 0
    assert await storage.purge(older_than=timedelta(seconds=-1)) == 1
    stats = await storage.stats()
    assert stats.pending == 1
    storage.close()


async def test_purge_is_noop_with_delete_on_ack(tmp_path: Path) -> None:
    storage = SqliteStorage(str(tmp_path / "x.db"), delete_on_ack=True)
    storage.create_schema()
    mid = await storage.add("t", b"x")
    await storage.claim(batch_size=1, lease=LEASE, worker_id="w")
    await storage.ack([mid], worker_id="w")
    assert await storage.purge(older_than=timedelta(0)) == 0
    assert (await storage.stats()).pending == 0
    storage.close()
