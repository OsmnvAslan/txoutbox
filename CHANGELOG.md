# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/OsmnvAslan/txoutbox/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/OsmnvAslan/txoutbox/releases/tag/v0.1.0
