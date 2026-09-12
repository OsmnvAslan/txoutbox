"""Outbox storage on PostgreSQL via ``asyncpg``.

Claims use ``FOR UPDATE SKIP LOCKED`` so any number of relay workers can share one
table without stepping on each other. Install with ``pip install txoutbox[postgres]``.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any, Self

from ..message import MessageId, OutboxMessage

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
CREATE INDEX IF NOT EXISTS {index} ON {table} (id) WHERE status = 'pending';
"""


class PostgresStorage:
    """Outbox table in PostgreSQL, driven by an :class:`asyncpg.Pool`.

    Pass your application's pool, or build a private one with :meth:`connect`.
    ``strict_ordering`` (default) holds back a message while an earlier pending message
    with the same key is not claimable (leased elsewhere, or waiting for its retry).
    ``delete_on_ack`` removes delivered rows instead of marking them ``done``.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        table: str = "outbox",
        schema: str | None = None,
        strict_ordering: bool = True,
        delete_on_ack: bool = False,
    ) -> None:
        if not table.isidentifier():
            raise ValueError("table must be a plain identifier")
        if schema is not None and not schema.isidentifier():
            raise ValueError("schema must be a plain identifier")
        self.pool = pool
        self.table = table
        self.schema = schema
        self.strict_ordering = strict_ordering
        self.delete_on_ack = delete_on_ack
        self._owns_pool = False
        self._sql = _Sql(self.qualified_table, f"{table}_pending_idx", strict_ordering)

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
        """DDL for the outbox table. Paste it into your migration tool."""
        return SCHEMA_SQL.format(table=self.qualified_table, index=f"{self.table}_pending_idx")

    async def create_schema(self) -> None:
        await self.pool.execute(self.schema_sql())

    # -- producer side -----------------------------------------------------------------

    async def insert(
        self,
        conn: asyncpg.Connection,
        topic: str,
        payload: bytes,
        *,
        key: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> int:
        """Insert a row using *your* connection, inside *your* transaction."""
        row_id: int = await conn.fetchval(
            self._sql.insert, topic, key, payload, json.dumps(dict(headers or {}))
        )
        return row_id

    async def add(
        self,
        topic: str,
        payload: bytes,
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
        rows = await self.pool.fetch(self._sql.claim, worker_id, lease, batch_size)
        return [_row_to_message(row) for row in rows]

    async def ack(self, ids: Sequence[MessageId]) -> None:
        if not ids:
            return
        sql = self._sql.delete if self.delete_on_ack else self._sql.ack
        await self.pool.execute(sql, list(ids))

    async def nack(self, message_id: MessageId, *, error: str, retry_at: datetime) -> None:
        await self.pool.execute(self._sql.nack, retry_at, error, message_id)

    async def dead_letter(self, message_id: MessageId, *, error: str) -> None:
        await self.pool.execute(self._sql.dead_letter, error, message_id)


_INIT_KWARGS = frozenset({"table", "schema", "strict_ordering", "delete_on_ack"})


class _Sql:
    """Statements pre-rendered for one table."""

    def __init__(self, table: str, index: str, strict_ordering: bool) -> None:
        blocked = (
            " AND (o.key IS NULL OR NOT EXISTS ("
            f"SELECT 1 FROM {table} p WHERE p.key = o.key AND p.id < o.id"
            " AND p.status = 'pending' AND (p.retry_at > now() OR p.lease_until > now())))"
            if strict_ordering
            else ""
        )
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
            f"{blocked} ORDER BY o.id LIMIT $3 FOR UPDATE SKIP LOCKED)"
            " RETURNING id, topic, key, payload, headers::text AS headers, attempts, created_at"
        )
        self.ack = (
            f"UPDATE {table} SET status = 'done', leased_by = NULL, lease_until = NULL"
            " WHERE id = ANY($1::bigint[])"
        )
        self.delete = f"DELETE FROM {table} WHERE id = ANY($1::bigint[])"
        self.nack = (
            f"UPDATE {table} SET retry_at = $1, last_error = $2, leased_by = NULL,"
            " lease_until = NULL WHERE id = $3"
        )
        self.dead_letter = (
            f"UPDATE {table} SET status = 'dead', last_error = $1, leased_by = NULL,"
            " lease_until = NULL WHERE id = $2"
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
