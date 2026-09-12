from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from txoutbox.adapters.memory import MemoryStorage
from txoutbox.adapters.sqlite import SqliteStorage
from txoutbox.testing import StorageContract


class TestMemoryStorage(StorageContract):
    @pytest.fixture
    def storage(self) -> MemoryStorage:
        return MemoryStorage()

    async def insert(self, storage, topic, payload, key=None):
        return storage.add(topic, payload, key=key)


class TestSqliteStorage(StorageContract):
    @pytest.fixture
    async def storage(self, tmp_path: Path) -> AsyncIterator[SqliteStorage]:
        s = SqliteStorage(str(tmp_path / "outbox.db"))
        s.create_schema()
        yield s
        s.close()

    async def insert(self, storage, topic, payload, key=None):
        return await storage.add(topic, payload, key=key)


class TestSqliteStorageDeleteOnAck(TestSqliteStorage):
    @pytest.fixture
    async def storage(self, tmp_path: Path) -> AsyncIterator[SqliteStorage]:
        s = SqliteStorage(str(tmp_path / "outbox.db"), delete_on_ack=True)
        s.create_schema()
        yield s
        s.close()


class TestSqliteStorageLoose(TestSqliteStorage):
    strict_ordering = False

    @pytest.fixture
    async def storage(self, tmp_path: Path) -> AsyncIterator[SqliteStorage]:
        s = SqliteStorage(str(tmp_path / "outbox.db"), strict_ordering=False)
        s.create_schema()
        yield s
        s.close()


def test_sqlite_rejects_bad_table_name(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        SqliteStorage(str(tmp_path / "x.db"), table="outbox; drop table users")


async def test_sqlite_insert_inside_user_transaction(tmp_path: Path) -> None:
    import sqlite3

    path = str(tmp_path / "app.db")
    storage = SqliteStorage(path)
    storage.create_schema()
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")
    with conn:  # one transaction: business row + outbox row
        conn.execute("INSERT INTO orders (total) VALUES (42)")
        storage.insert(conn, "orders.created", b'{"total": 42}', key="order-1", headers={"v": "1"})
    got = await storage.claim(
        batch_size=10, lease=__import__("datetime").timedelta(seconds=1), worker_id="w"
    )
    assert len(got) == 1 and got[0].headers == {"v": "1"} and got[0].created_at is not None
    conn.close()
    storage.close()
