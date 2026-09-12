"""Outbox storage on the standard library's ``sqlite3``.

Meant for tests, tooling and single-process apps. Calls run in a worker thread so
the event loop is never blocked; a process-wide lock serialises them, and every
claim runs under ``BEGIN IMMEDIATE``, so strict ordering is race-free here by
construction.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from ..message import MessageId, OutboxMessage, OutboxStats, Payload, encode_payload

SCHEMA = """
CREATE TABLE IF NOT EXISTS {table} (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    topic       TEXT    NOT NULL,
    key         TEXT,
    payload     BLOB    NOT NULL,
    headers     TEXT    NOT NULL DEFAULT '{{}}',
    status      TEXT    NOT NULL DEFAULT 'pending',
    attempts    INTEGER NOT NULL DEFAULT 0,
    retry_at    REAL,
    leased_by   TEXT,
    lease_until REAL,
    last_error  TEXT,
    created_at  REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS {table}_pending_idx ON {table} (status, id);
CREATE INDEX IF NOT EXISTS {table}_key_pending_idx ON {table} (key, id) WHERE status = 'pending';
"""


class SqliteStorage:
    """Outbox table in a SQLite database.

    ``strict_ordering`` (default) holds back a message while an earlier pending message
    with the same key is not claimable (leased elsewhere, or waiting for its retry).
    ``delete_on_ack`` removes delivered rows instead of marking them ``done``; otherwise
    call :meth:`purge` now and then to keep the table small.
    """

    def __init__(
        self,
        path: str,
        *,
        table: str = "outbox",
        strict_ordering: bool = True,
        delete_on_ack: bool = False,
    ) -> None:
        if not table.isidentifier():
            raise ValueError("table must be a plain identifier")
        self.path = path
        self.table = table
        self.strict_ordering = strict_ordering
        self.delete_on_ack = delete_on_ack
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")

    # -- setup -------------------------------------------------------------------------

    def schema_sql(self) -> str:
        """The DDL for the outbox table, for your migration tool."""
        return SCHEMA.format(table=self.table)

    def create_schema(self) -> None:
        with self._lock:
            self._conn.executescript(self.schema_sql())

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- producer side -----------------------------------------------------------------

    def insert(
        self,
        conn: sqlite3.Connection,
        topic: str,
        payload: Payload,
        *,
        key: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> MessageId:
        """Insert a row using *your* connection, inside *your* transaction."""
        cur = conn.execute(
            f"INSERT INTO {self.table} (topic, key, payload, headers, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (topic, key, encode_payload(payload), json.dumps(dict(headers or {})), _now()),
        )
        return cur.lastrowid

    async def add(
        self,
        topic: str,
        payload: Payload,
        *,
        key: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> MessageId:
        """Insert a row on the storage's own connection. Handy for demos and tests."""

        def _add() -> MessageId:
            with self._lock:
                return self.insert(self._conn, topic, payload, key=key, headers=headers)

        return await asyncio.to_thread(_add)

    # -- Storage protocol --------------------------------------------------------------

    async def claim(
        self, *, batch_size: int, lease: timedelta, worker_id: str
    ) -> Sequence[OutboxMessage]:
        return await asyncio.to_thread(self._claim, batch_size, lease, worker_id)

    async def ack(self, ids: Sequence[MessageId], *, worker_id: str) -> None:
        if not ids:
            return
        if self.delete_on_ack:
            sql = f"DELETE FROM {self.table} WHERE id = ? AND leased_by = ?"
        else:
            sql = (
                f"UPDATE {self.table} SET status = 'done', leased_by = NULL, lease_until = NULL"
                " WHERE id = ? AND leased_by = ?"
            )
        await asyncio.to_thread(self._executemany, sql, [(i, worker_id) for i in ids])

    async def nack(
        self, message_id: MessageId, *, worker_id: str, error: str, retry_at: datetime
    ) -> None:
        await asyncio.to_thread(
            self._execute,
            f"UPDATE {self.table} SET retry_at = ?, last_error = ?, leased_by = NULL,"
            " lease_until = NULL WHERE id = ? AND leased_by = ?",
            (retry_at.timestamp(), error, message_id, worker_id),
        )

    async def release(
        self, ids: Sequence[MessageId], *, worker_id: str, retry_at: datetime
    ) -> None:
        if not ids:
            return
        await asyncio.to_thread(
            self._executemany,
            f"UPDATE {self.table} SET attempts = attempts - 1, retry_at = ?, leased_by = NULL,"
            " lease_until = NULL WHERE id = ? AND leased_by = ?",
            [(retry_at.timestamp(), i, worker_id) for i in ids],
        )

    async def dead_letter(self, message_id: MessageId, *, worker_id: str, error: str) -> None:
        await asyncio.to_thread(
            self._execute,
            f"UPDATE {self.table} SET status = 'dead', last_error = ?, leased_by = NULL,"
            " lease_until = NULL WHERE id = ? AND leased_by = ?",
            (error, message_id, worker_id),
        )

    # -- operations --------------------------------------------------------------------

    async def stats(self) -> OutboxStats:
        return await asyncio.to_thread(self._stats)

    async def purge(self, older_than: timedelta) -> int:
        """Delete ``done`` rows created more than ``older_than`` ago. Returns the count."""
        if self.delete_on_ack:
            return 0
        cutoff = _now() - older_than.total_seconds()

        def _purge() -> int:
            with self._lock:
                cur = self._conn.execute(
                    f"DELETE FROM {self.table} WHERE status = 'done' AND created_at < ?",
                    (cutoff,),
                )
                return cur.rowcount

        return await asyncio.to_thread(_purge)

    # -- internals ---------------------------------------------------------------------

    def _claim(self, batch_size: int, lease: timedelta, worker_id: str) -> list[OutboxMessage]:
        t = self.table
        now = _now()
        blocked = (
            f" AND (o.key IS NULL OR NOT EXISTS (SELECT 1 FROM {t} p WHERE p.key = o.key"
            " AND p.id < o.id AND p.status = 'pending'"
            " AND (p.retry_at > :now OR p.lease_until > :now)))"
            if self.strict_ordering
            else ""
        )
        select = (
            f"SELECT o.id FROM {t} o WHERE o.status = 'pending'"
            " AND (o.retry_at IS NULL OR o.retry_at <= :now)"
            " AND (o.lease_until IS NULL OR o.lease_until <= :now)"
            f"{blocked} ORDER BY o.id LIMIT :limit"
        )
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                ids = [r[0] for r in self._conn.execute(select, {"now": now, "limit": batch_size})]
                if not ids:
                    self._conn.execute("COMMIT")
                    return []
                marks = ",".join("?" * len(ids))
                self._conn.execute(
                    f"UPDATE {t} SET attempts = attempts + 1, leased_by = ?, lease_until = ?,"
                    f" retry_at = NULL WHERE id IN ({marks})",
                    (worker_id, now + lease.total_seconds(), *ids),
                )
                rows = self._conn.execute(
                    f"SELECT id, topic, key, payload, headers, attempts, created_at FROM {t}"
                    f" WHERE id IN ({marks}) ORDER BY id",
                    ids,
                ).fetchall()
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
        return [_row_to_message(row) for row in rows]

    def _stats(self) -> OutboxStats:
        t = self.table
        with self._lock:
            pending, oldest = self._conn.execute(
                f"SELECT COUNT(*), MIN(created_at) FROM {t} WHERE status = 'pending'"
            ).fetchone()
            (dead,) = self._conn.execute(
                f"SELECT COUNT(*) FROM {t} WHERE status = 'dead'"
            ).fetchone()
        age = None if oldest is None else timedelta(seconds=max(0.0, _now() - oldest))
        return OutboxStats(pending=pending, oldest_pending_age=age, dead=dead)

    def _execute(self, sql: str, params: tuple[Any, ...]) -> None:
        with self._lock:
            self._conn.execute(sql, params)

    def _executemany(self, sql: str, params: list[tuple[Any, ...]]) -> None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.executemany(sql, params)
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise


def _now() -> float:
    return datetime.now(UTC).timestamp()


def _row_to_message(row: tuple[Any, ...]) -> OutboxMessage:
    message_id, topic, key, payload, headers, attempts, created_at = row
    return OutboxMessage(
        id=message_id,
        topic=topic,
        payload=bytes(payload),
        key=key,
        headers=json.loads(headers),
        attempts=attempts,
        created_at=datetime.fromtimestamp(created_at, tz=UTC),
    )
