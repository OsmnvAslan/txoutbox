# txoutbox

Storage-agnostic Transactional Outbox relay for asyncio. Zero runtime dependencies,
Python 3.11+, `src/` layout, uv + hatchling, ruff, `mypy --strict`, pytest-asyncio (auto).

```bash
uv sync --group dev --group examples --extra postgres
uv run pytest -q                 # Postgres tests use TXOUTBOX_PG_DSN or embedded pgserver (py<3.13)
uv run ruff check src tests examples && uv run ruff format --check src tests examples
uv run mypy src
```

## Invariants a change must not break

- **Fencing.** Every `Storage` write-back (`ack`, `nack`, `release`, `dead_letter`) only touches
  rows currently leased by its `worker_id`.
- **Attempts are counted at claim time.** `release` gives an attempt back; nothing else does.
- **Per-key order.** Messages sharing a key are published sequentially in id order; a failure
  releases the rest of its key untried. With strict ordering this holds across rounds, retries
  and concurrent workers (Postgres: advisory lock + `NOT EXISTS` predicate).
- **Leases bound duplicates.** A round must fit in its lease (`RelayConfig.worst_case_round`).
- **Cancellation safety.** `CancelledError` is always re-raised, never swallowed by `except
  Exception`; accepted messages are acked even when a round is cancelled (`asyncio.shield`).
- **Telemetry never breaks delivery.** Hook and bookkeeping errors are logged and swallowed.
- **Zero runtime dependencies** in the core; adapters import their driver lazily behind an extra.
- **The `Storage` protocol is public API.** Changing it is a breaking release; every contract
  rule has a test in `txoutbox.testing.StorageContract`.

## Conventions

- Every bug fix comes with a regression test that fails without the fix.
- User-visible changes go into `CHANGELOG.md` (Keep a Changelog), docs into `README.md`.
- Release = bump with `uv version X.Y.Z`, changelog entry, push tag `vX.Y.Z` on `main`.
