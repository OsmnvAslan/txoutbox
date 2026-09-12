"""Outbox storage on PostgreSQL via ``asyncpg``.

Claims use ``FOR UPDATE SKIP LOCKED`` so any number of relay workers can share one
table without stepping on each other. Install with ``pip install txoutbox[postgres]``.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from types import TracebackType
from typing import Any, Self

from ..message import MessageId, OutboxMessage, OutboxStats, Payload, encode_payload

try:
    import asyncpg
except ImportError as _exc:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        "txoutbox.adapters.postgres needs asyncpg: pip install 'txoutbox[postgres]'"
    ) from _exc

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    id          BIGSERIAL   PRIMARY KEY,
    topic       TEXT        NOT NULL,
    key         TEXT,
    payload     BYTEA       NOT NULL,
    headers     JSONB       NOT NULL DEFAULT '{{}}',
    status      TEXT        NOT NULL DEFAULT 'pending',
    attempts    INT         NOT NULL DEFAULT 0,
    retry_at    TIMESTAMPTZ,
    leased_by   TEXT,
    lease_until TIMESTAMPTZ,
    last_error  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS {name}_pending_idx ON {table} (id) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS {name}_key_pending_idx ON {table} (key, id) WHERE status = 'pending';
"""

NOTIFY_SQL = """
CREATE OR REPLACE FUNCTION {function}() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('{channel}', NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS {name}_notify ON {table};
CREATE TRIGGER {name}_notify AFTER INSERT ON {table}
    FOR EACH ROW EXECUTE FUNCTION {function}();
"""


class PostgresStorage:
    """Outbox table in PostgreSQL, driven by an :class:`asyncpg.Pool`.

    Pass your application's pool, or build a private one with :meth:`connect`.

    * ``strict_ordering`` (default) holds back a message while an earlier pending message
      with the same key is not claimable (leased elsewhere, or waiting for its retry).
      To make that predicate correct across concurrent workers, each claim takes a
      transaction-scoped advisory lock on the table, so claims are serialised. A claim is
      a single statement that takes milliseconds; publishing, the slow part, still runs
      in parallel on every worker.
    * ``delete_on_ack`` removes delivered rows instead of marking them ``done``.
    * ``visibility_delay`` claims only rows older than the given age. ``BIGSERIAL`` ids
      are assigned at insert time, not commit time, so a long transaction can commit a
      lower id *after* a higher one was already delivered. A delay of a few seconds
      (longer than your slowest producing transaction) makes that reordering impossible.
    * ``notify_channel`` adds an ``AFTER INSERT`` trigger to :meth:`schema_sql` that
      calls ``pg_notify``; pair it with :meth:`listen` to wake the relay instantly.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        table: str = "outbox",
        schema: str | None = None,
        strict_ordering: bool = True,
        delete_on_ack: bool = False,
        visibility_delay: timedelta | None = None,
        notify_channel: str | None = None,
    ) -> None:
        if not table.isidentifier():
            raise ValueError("table must be a plain identifier")
        if schema is not None and not schema.isidentifier():
            raise ValueError("schema must be a plain identifier")
        if notify_channel is not None and not notify_channel.isidentifier():
            raise ValueError("notify_channel must be a plain identifier")
        if visibility_delay is not None and visibility_delay < timedelta(0):
            raise ValueError("visibility_delay must not be negative")
        self.pool = pool
        self.table = table
        self.schema = schema
        self.strict_ordering = strict_ordering
        self.delete_on_ack = delete_on_ack
        self.visibility_delay = visibility_delay
        self.notify_channel = notify_channel
        self._owns_pool = False
        self._sql = _Sql(self.qualified_table, strict_ordering, visibility_delay is not None)

    @classmethod
    async def connect(cls, dsn: str, **kwargs: Any) -> Self:
        """Create a storage with its own pool. ``kwargs`` go to the constructor."""
        pool_kwargs = {k: kwargs.pop(k) for k in list(kwargs) if k not in _INIT_KWARGS}
        pool = await asyncpg.create_pool(dsn, **pool_kwargs)
        self = cls(pool, **kwargs)
        self._owns_pool = True
        return self

    async def close(self) -> None:
        """Close the pool, but only if :meth:`connect` created it."""
        if self._owns_pool:
            await self.pool.close()
            self._owns_pool = False

    @property
    def qualified_table(self) -> str:
        return f"{self.schema}.{self.table}" if self.schema else self.table

    # -- setup -------------------------------------------------------------------------

    def schema_sql(self) -> str:
        """DDL for the outbox table (and the notify trigger). Paste it into your migrations."""
        sql = SCHEMA_SQL.format(table=self.qualified_table, name=self.table)
        if self.notify_channel:
            function = f"{self.qualified_table}_notify"
            sql += NOTIFY_SQL.format(
                table=self.qualified_table,
                name=self.table,
                function=function,
                channel=self.notify_channel,
            )
        return sql

    async def create_schema(self) -> None:
        await self.pool.execute(self.schema_sql())

    # -- producer side -----------------------------------------------------------------

    async def insert(
        self,
        conn: asyncpg.Connection,
        topic: str,
        payload: Payload,
        *,
        key: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> int:
        """Insert a row using *your* connection, inside *your* transaction.

        ``payload`` may be ``bytes``, ``str`` or a JSON-serialisable dict/list.
        """
        row_id: int = await conn.fetchval(
            self._sql.insert,
            topic,
            key,
            encode_payload(payload),
            json.dumps(dict(headers or {})),
        )
        return row_id

    async def add(
        self,
        topic: str,
        payload: Payload,
        *,
        key: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> int:
        """Insert a row on a pooled connection. Handy for demos and tests."""
        async with self.pool.acquire() as conn:
            return await self.insert(conn, topic, payload, key=key, headers=headers)

    # -- Storage protocol --------------------------------------------------------------

    async def claim(
        self, *, batch_size: int, lease: timedelta, worker_id: str
    ) -> Sequence[OutboxMessage]:
        args: list[Any] = [worker_id, lease, batch_size]
        if self.visibility_delay is not None:
            args.append(self.visibility_delay)
        async with self.pool.acquire() as conn, conn.transaction():
            if self.strict_ordering:
                await conn.execute(self._sql.lock, self._sql.lock_key)
            rows = await conn.fetch(self._sql.claim, *args)
        return [_row_to_message(row) for row in sorted(rows, key=lambda r: r["id"])]

    async def ack(self, ids: Sequence[MessageId], *, worker_id: str) -> None:
        if not ids:
            return
        sql = self._sql.delete if self.delete_on_ack else self._sql.ack
        await self.pool.execute(sql, list(ids), worker_id)

    async def nack(
        self, message_id: MessageId, *, worker_id: str, error: str, retry_at: datetime
    ) -> None:
        await self.pool.execute(self._sql.nack, retry_at, error, message_id, worker_id)

    async def release(
        self, ids: Sequence[MessageId], *, worker_id: str, retry_at: datetime
    ) -> None:
        if not ids:
            return
        await self.pool.execute(self._sql.release, retry_at, list(ids), worker_id)

    async def dead_letter(self, message_id: MessageId, *, worker_id: str, error: str) -> None:
        await self.pool.execute(self._sql.dead_letter, error, message_id, worker_id)

    # -- operations --------------------------------------------------------------------

    async def stats(self) -> OutboxStats:
        row = await self.pool.fetchrow(self._sql.stats)
        return OutboxStats(
            pending=row["pending"], oldest_pending_age=row["oldest"], dead=row["dead"]
        )

    async def purge(self, older_than: timedelta) -> int:
        """Delete ``done`` rows older than ``older_than``. Returns the number deleted."""
        if self.delete_on_ack:
            return 0
        status: str = await self.pool.execute(self._sql.purge, older_than)
        return int(status.rsplit(" ", 1)[-1])

    async def listen(self, callback: Callable[[], None]) -> Listener:
        """Call ``callback()`` on every ``NOTIFY`` on ``notify_channel``.

        Holds one pooled connection until :meth:`Listener.close`. Typical use::

            listener = await storage.listen(relay.wake)
            ...
            await listener.close()
        """
        if not self.notify_channel:
            raise ValueError("listen() needs notify_channel")
        conn = await self.pool.acquire()
        try:
            await conn.add_listener(self.notify_channel, lambda *_: callback())
        except BaseException:
            await self.pool.release(conn)
            raise
        return Listener(self.pool, conn, self.notify_channel)


class Listener:
    """Handle returned by :meth:`PostgresStorage.listen`. Also an async context manager."""

    def __init__(self, pool: asyncpg.Pool, conn: asyncpg.Connection, channel: str) -> None:
        self._pool = pool
        self._conn: asyncpg.Connection | None = conn
        self._channel = channel

    async def close(self) -> None:
        conn, self._conn = self._conn, None
        if conn is None:
            return
        try:
            await conn.reset()  # drops listeners
        finally:
            await self._pool.release(conn)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()


_INIT_KWARGS = frozenset(
    {"table", "schema", "strict_ordering", "delete_on_ack", "visibility_delay", "notify_channel"}
)


class _Sql:
    """Statements pre-rendered for one table."""

    def __init__(self, table: str, strict_ordering: bool, visibility_delay: bool) -> None:
        blocked = (
            " AND (o.key IS NULL OR NOT EXISTS ("
            f"SELECT 1 FROM {table} p WHERE p.key = o.key AND p.id < o.id"
            " AND p.status = 'pending' AND (p.retry_at > now() OR p.lease_until > now())))"
            if strict_ordering
            else ""
        )
        visible = " AND o.created_at <= now() - $4::interval" if visibility_delay else ""
        self.lock_key = f"txoutbox:{table}"
        self.lock = "SELECT pg_advisory_xact_lock(hashtext($1))"
        self.insert = (
            f"INSERT INTO {table} (topic, key, payload, headers)"
            " VALUES ($1, $2, $3, $4::jsonb) RETURNING id"
        )
        self.claim = (
            f"UPDATE {table} SET attempts = attempts + 1, leased_by = $1,"
            " lease_until = now() + $2, retry_at = NULL"
            f" WHERE id IN (SELECT o.id FROM {table} o WHERE o.status = 'pending'"
            " AND (o.retry_at IS NULL OR o.retry_at <= now())"
            " AND (o.lease_until IS NULL OR o.lease_until <= now())"
            f"{visible}{blocked} ORDER BY o.id LIMIT $3 FOR UPDATE SKIP LOCKED)"
            " RETURNING id, topic, key, payload, headers::text AS headers, attempts, created_at"
        )
        self.ack = (
            f"UPDATE {table} SET status = 'done', leased_by = NULL, lease_until = NULL"
            " WHERE id = ANY($1::bigint[]) AND leased_by = $2"
        )
        self.delete = f"DELETE FROM {table} WHERE id = ANY($1::bigint[]) AND leased_by = $2"
        self.nack = (
            f"UPDATE {table} SET retry_at = $1, last_error = $2, leased_by = NULL,"
            " lease_until = NULL WHERE id = $3 AND leased_by = $4"
        )
        self.release = (
            f"UPDATE {table} SET attempts = attempts - 1, retry_at = $1, leased_by = NULL,"
            " lease_until = NULL WHERE id = ANY($2::bigint[]) AND leased_by = $3"
        )
        self.dead_letter = (
            f"UPDATE {table} SET status = 'dead', last_error = $1, leased_by = NULL,"
            " lease_until = NULL WHERE id = $2 AND leased_by = $3"
        )
        self.stats = (
            "SELECT count(*) FILTER (WHERE status = 'pending') AS pending,"
            " now() - min(created_at) FILTER (WHERE status = 'pending') AS oldest,"
            f" count(*) FILTER (WHERE status = 'dead') AS dead FROM {table}"
        )
        self.purge = (
            f"DELETE FROM {table} WHERE status = 'done' AND created_at < now() - $1::interval"
        )


def _row_to_message(row: asyncpg.Record) -> OutboxMessage:
    return OutboxMessage(
        id=row["id"],
        topic=row["topic"],
        payload=bytes(row["payload"]),
        key=row["key"],
        headers=json.loads(row["headers"]),
        attempts=row["attempts"],
        created_at=row["created_at"],
    )
