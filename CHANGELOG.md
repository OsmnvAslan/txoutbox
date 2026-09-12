# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2] - 2026-09-12

### Fixed

- A second SIGINT/SIGTERM that arrived while `run()` was pausing after a storage
  error leaked `CancelledError` instead of returning quietly; the cancellation guard
  now covers the whole loop body.
- The pause after a storage error is now cut short by `stop()` and by the first
  signal, so a worker whose database is down still shuts down promptly.
- `AdaptivePoller.wait()` accepts an explicit `timeout`.

### Changed

- Docs: `listen()` holds one pool connection; Kafka snippet is valid code.
- GitHub Actions bumped to current majors.

## [0.2.1] - 2026-09-12

### Fixed

- Default `RelayConfig` could not keep its own promise: the worst-case round
  (`ceil(batch_size / concurrency) * publish_timeout`) was 100 s against a 30 s lease.
  Defaults are now `batch_size=50`, `lease=60s`, `publish_timeout=5s` (worst case 25 s),
  and `RelayConfig` logs a warning with the arithmetic when a custom config breaks the
  invariant. `RelayConfig.worst_case_round` exposes the estimate.
- `run(handle_signals=True)` claimed to restore previous signal handlers, but handlers
  registered with `loop.add_signal_handler` (uvicorn, hypercorn) cannot be restored and
  were silently lost. Only plain `signal.signal` handlers are put back now, and the
  docs say `handle_signals` is for a process the relay owns.
- A second SIGINT/SIGTERM now makes `run()` return quietly instead of leaking
  `CancelledError` out of `asyncio.run`.
- `PostgresStorage.listen()` with `visibility_delay` woke the relay before the row was
  visible; the wake-up is now deferred by the delay.
- `PostgresStorage.listen()` reconnects when the connection drops instead of dying
  silently. The relay polls in the meantime.
- `PostgresStorage.stats()` uses two index-friendly queries instead of a table scan.
- The first failed publish of a message is logged at INFO with the error text; later
  attempts stay at DEBUG. The per-round summary remains at WARNING.
- Kafka snippet in the README now starts the producer.

### Changed

- `StorageContract.short_lease` lets adapter authors raise the expiry-test lease for a
  slow or remote database.
- `pytest-timeout` is applied on the CI command line rather than in `pyproject.toml`,
  so running the sdist's tests without the plugin does not warn.
- Docs: advisory-lock throughput ceiling, clock skew note, `purge()` uses `created_at`.

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

[Unreleased]: https://github.com/OsmnvAslan/txoutbox/compare/v0.2.2...HEAD
[0.2.2]: https://github.com/OsmnvAslan/txoutbox/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/OsmnvAslan/txoutbox/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/OsmnvAslan/txoutbox/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/OsmnvAslan/txoutbox/releases/tag/v0.1.0
