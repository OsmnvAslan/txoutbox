"""Contract tests for PostgresStorage.

Uses ``TXOUTBOX_PG_DSN`` when set; otherwise starts an embedded PostgreSQL through
the ``pgserver`` package. Skips when neither is available.
"""

import os
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any, ClassVar

import pytest

asyncpg = pytest.importorskip("asyncpg")

from txoutbox.adapters.postgres import PostgresStorage  # noqa: E402
from txoutbox.testing import StorageContract  # noqa: E402

SCRATCH = Path(
    "/private/tmp/claude-501/-Users-aslanosmanov-Projects-open/"
    "d1be8335-04c6-4fa8-af9f-4a106e52bdef/scratchpad"
)


@pytest.fixture(scope="session")
def dsn() -> Iterator[str]:
    if env := os.environ.get("TXOUTBOX_PG_DSN"):
        yield env
        return
    try:
        import pgserver
    except ImportError:
        pytest.skip("set TXOUTBOX_PG_DSN or install pgserver to run Postgres tests")
    pgdata = (SCRATCH if SCRATCH.is_dir() else Path.cwd() / ".pgdata") / "txoutbox-tests"
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
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
    yield pool
    await pool.close()


class _PostgresContract(StorageContract):
    kwargs: ClassVar[dict[str, Any]] = {}

    @pytest.fixture
    async def storage(self, pool: "asyncpg.Pool") -> AsyncIterator[PostgresStorage]:
        storage = PostgresStorage(pool, table=f"outbox_{uuid.uuid4().hex[:8]}", **self.kwargs)
        await storage.create_schema()
        yield storage
        await pool.execute(f"DROP TABLE {storage.qualified_table}")

    async def insert(self, storage, topic, payload, key=None):
        return await storage.add(topic, payload, key=key)


class TestPostgresStorage(_PostgresContract):
    pass


class TestPostgresStorageDeleteOnAck(_PostgresContract):
    kwargs: ClassVar[dict[str, Any]] = {"delete_on_ack": True}

    async def test_ack_deletes_rows(self, storage: PostgresStorage, pool) -> None:
        await storage.add("t", b"p")
        got = await storage.claim(batch_size=10, lease=timedelta(seconds=30), worker_id="w")
        await storage.ack([m.id for m in got])
        assert await pool.fetchval(f"SELECT count(*) FROM {storage.qualified_table}") == 0


class TestPostgresStorageLoose(_PostgresContract):
    strict_ordering = False
    kwargs: ClassVar[dict[str, Any]] = {"strict_ordering": False}


async def test_insert_inside_user_transaction(pool) -> None:
    table = f"outbox_{uuid.uuid4().hex[:8]}"
    storage = PostgresStorage(pool, table=table)
    await storage.create_schema()
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
    got = await storage.claim(batch_size=10, lease=timedelta(seconds=30), worker_id="w")
    assert [m.id for m in got] == [row_id]
    assert got[0].payload == b'{"total": 42}'
    assert got[0].headers == {"v": "1"}
    assert got[0].key == "o2"
    assert got[0].created_at is not None and got[0].created_at.tzinfo is not None
    await pool.execute(f"DROP TABLE {table}")


class _Abort(Exception):
    pass


async def test_connect_owns_and_closes_pool(dsn: str) -> None:
    table = f"outbox_{uuid.uuid4().hex[:8]}"
    storage = await PostgresStorage.connect(dsn, table=table, min_size=1, max_size=2)
    await storage.create_schema()
    await storage.add("t", b"p")
    assert len(await storage.claim(batch_size=1, lease=timedelta(seconds=1), worker_id="w")) == 1
    await storage.pool.execute(f"DROP TABLE {table}")
    await storage.close()
    assert storage.pool.is_closing()


async def test_schema_argument_and_sql(pool) -> None:
    schema = f"s_{uuid.uuid4().hex[:8]}"
    await pool.execute(f"CREATE SCHEMA {schema}")
    storage = PostgresStorage(pool, table="outbox", schema=schema)
    assert storage.qualified_table == f"{schema}.outbox"
    assert f"CREATE TABLE IF NOT EXISTS {schema}.outbox" in storage.schema_sql()
    await storage.create_schema()
    await storage.add("t", b"p")
    assert len(await storage.claim(batch_size=5, lease=timedelta(seconds=1), worker_id="w")) == 1
    await pool.execute(f"DROP SCHEMA {schema} CASCADE")


def test_identifier_validation() -> None:
    with pytest.raises(ValueError):
        PostgresStorage(object(), table="outbox; drop table users")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        PostgresStorage(object(), schema="public; --")  # type: ignore[arg-type]
