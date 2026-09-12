# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-09-12

Breaking release of the `Storage` protocol. Nothing depended on 0.1.0 yet, so the
guarantees were fixed instead of patched.

### Fixed

- Messages held back behind a failing head no longer burn an attempt and no longer get a
  spurious `last_error`. They are handed back through the new `Storage.release()`.
- Strict per-key ordering on Postgres was racy between concurrent workers: an uncommitted
  claim was invisible to the other worker's `NOT EXISTS` check. Claims now take a
  transaction-scoped advisory lock (publishing still runs in parallel).
- `ack`/`nack`/`dead_letter` were not fenced: a late write from a worker whose lease had
  expired could clear another worker's lease. Every write-back now carries `worker_id`
  and only touches rows that worker still holds.
- `UPDATE ... RETURNING` order is not guaranteed; claimed rows are now sorted by id.
- The adaptive poller stayed at its maximum interval after a burst of full batches.
- Messages the publisher accepted are now acked even if the round is cancelled.
- A second SIGINT/SIGTERM cancels the round instead of waiting for a hanging publisher.
  Previous signal handlers are restored on exit.
- `Backoff.delay()` no longer overflows for large attempt numbers.

### Added

- `Storage.release(ids, *, worker_id, retry_at)`.
- `Relay` accepts a plain `async def publish(message)` function as the publisher.
- Payloads may be `bytes`, `str` or JSON-serialisable objects; `OutboxMessage.json()` / `.text()`.
- `stats()` on all built-in adapters (`pending`, `oldest_pending_age`, `dead`) and the
  `StatsProvider` protocol; `Hooks.on_round(result, duration)`.
- `purge(older_than)` on SQL adapters; `visibility_delay`, `notify_channel` and
  `listen(callback)` on `PostgresStorage`.
- `(key, id) WHERE status = 'pending'` index in both SQL schemas.
- Contract tests for fencing, `release`, `stats` and strict ordering under concurrent claims.
- A Django ORM storage recipe under `examples/`.
- `publish_timeout` defaults to 10 s; a warning is logged when a round outlives its lease.
- `RelayConfig` validates `publish_timeout` and `storage_error_delay`.

### Changed

- `MemoryStorage.add()` is async like the other adapters (`add_sync()` remains).
- `MessageId` is `Hashable` instead of `Any`.
- Per-message retry logging moved to DEBUG; one WARNING summary per round with failures.
- `asyncpg>=0.30` for the `postgres` extra.

## [0.1.0] - 2026-09-12

### Added

- `Relay`: storage-agnostic Transactional Outbox relay for asyncio, with
  adaptive polling that backs off while the table is empty and snaps back when
  work appears, and a `wake()` hook for LISTEN/NOTIFY-style triggers.
- Per-key ordering: messages sharing a key are published sequentially in claim
  order, while different keys are published concurrently up to `concurrency`.
  A failure blocks the rest of its key group for the round so retries never
  reorder a key.
- Retries with exponential backoff and full jitter (`Backoff`), attempt
  counting at claim time, and dead-lettering after `max_attempts`.
- Leases: claimed rows are invisible to other workers until the lease expires,
  so a crashed worker's batch is recovered automatically.
- Graceful shutdown: `stop()`, optional SIGINT/SIGTERM handling, and an async
  context manager that runs the relay as a background task.
- `Hooks` for observability (`on_claimed`, `on_published`, `on_retry`,
  `on_dead_letter`, `on_blocked`); hook errors never break delivery.
- Adapters: `MemoryStorage` and `MemoryPublisher` for tests and demos,
  `SqliteStorage` on the standard library, and `PostgresStorage` on `asyncpg`
  using `FOR UPDATE SKIP LOCKED` (extra `postgres`).
- `txoutbox.testing.StorageContract`: a reusable test suite that verifies any
  `Storage` implementation against the relay's expectations, including lease
  expiry, concurrent claims and strict per-key ordering.

[Unreleased]: https://github.com/OsmnvAslan/txoutbox/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/OsmnvAslan/txoutbox/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/OsmnvAslan/txoutbox/releases/tag/v0.1.0
