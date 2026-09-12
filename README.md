# txoutbox

**Storage-agnostic Transactional Outbox relay for asyncio.** Bring your own table, bring your own broker.

[![PyPI](https://img.shields.io/pypi/v/txoutbox)](https://pypi.org/project/txoutbox/)
[![Python](https://img.shields.io/pypi/pyversions/txoutbox)](https://pypi.org/project/txoutbox/)
[![CI](https://github.com/OsmnvAslan/txoutbox/actions/workflows/ci.yml/badge.svg)](https://github.com/OsmnvAslan/txoutbox/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Zero dependencies. Fully typed. Python 3.11+.

## The problem

Your service writes to the database and must tell the world about it: Kafka, NSQ, RabbitMQ, a webhook.
Publishing straight from request code is unreliable. If the broker is down after commit, the event is lost.
If you publish before commit and the transaction rolls back, you announced something that never happened.

The Transactional Outbox pattern fixes that: write the event to an `outbox` table **in the same transaction**
as your business data, and let a separate relay process deliver it. The idea is simple. The relay is not.
Every team writes one, and every team hits the same bugs: two workers grab the same row, a crashed worker
holds a row forever, events for one order get reordered, an empty table gets polled every 10 ms, retries
hammer a broker that is already down, and `SIGTERM` kills the process mid-batch.

Existing libraries solve this bundled with infrastructure: their table, their ORM, their broker, sometimes
their whole framework. If your stack is Django ORM + NSQ, raw asyncpg + webhooks, or an outbox table you
are not allowed to change, you are back to writing your own relay.

## What txoutbox does

Only the relay. You implement two small protocols, and txoutbox handles the hard parts:

| Concern | How |
| --- | --- |
| Concurrent workers | Leases: `claim()` hands each row to one worker for a limited time |
| Crashed workers | Leases expire, the row is claimed again, the attempt is counted |
| Ordering | Messages sharing a `key` are published sequentially, in insertion order; different keys run in parallel |
| Retries | Exponential backoff with full jitter, per message, scheduled in storage |
| Poison messages | Dead-lettered after `max_attempts` |
| Idle polling | Adaptive: sleep grows from 50 ms to 5 s while the table is empty, resets on work; `wake()` for LISTEN/NOTIFY |
| Broker hangs | Optional per-message `publish_timeout` |
| Shutdown | `SIGTERM`/`SIGINT` finish the round in flight, then return |
| Observability | `Hooks` with `on_published`, `on_retry`, `on_dead_letter`, `on_blocked`, `on_claimed` |

## Install

```bash
pip install txoutbox              # core, zero dependencies
pip install "txoutbox[postgres]"  # + asyncpg adapter
```

## 60 seconds

```python
import asyncio, sqlite3
from txoutbox import Relay, OutboxMessage
from txoutbox.adapters.sqlite import SqliteStorage

storage = SqliteStorage("app.db")
storage.create_schema()

# --- producer side: business row + outbox row, one transaction ---
conn = sqlite3.connect("app.db")
conn.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, total INTEGER)")
with conn:
    cur = conn.execute("INSERT INTO orders (total) VALUES (42)")
    storage.insert(conn, "orders.created", b'{"total": 42}', key=f"order-{cur.lastrowid}")

# --- relay side: your publisher is two lines ---
class PrintPublisher:
    async def publish(self, message: OutboxMessage) -> None:
        print(message.topic, message.key, message.payload)

async def main():
    relay = Relay(storage, PrintPublisher())
    await relay.run(handle_signals=True)   # Ctrl-C to stop

asyncio.run(main())
```

Swap `SqliteStorage` for `PostgresStorage`, or your own adapter, and `PrintPublisher` for Kafka, NSQ, an HTTP client.
The relay does not change.

## The two protocols

```python
class Storage(Protocol):
    async def claim(self, *, batch_size: int, lease: timedelta, worker_id: str) -> Sequence[OutboxMessage]: ...
    async def ack(self, ids: Sequence[MessageId]) -> None: ...
    async def nack(self, message_id: MessageId, *, error: str, retry_at: datetime) -> None: ...
    async def dead_letter(self, message_id: MessageId, *, error: str) -> None: ...

class Publisher(Protocol):
    async def publish(self, message: OutboxMessage) -> None: ...   # raise to retry
```

`OutboxMessage` carries `id`, `topic`, `payload: bytes`, optional `key`, `headers`, `attempts`, `created_at`.
The `id` is opaque: the relay only hands it back to your storage.

### Writing your own storage adapter

Implement the four methods over your table, then let the contract tests check the tricky bits
(lease expiry, concurrent claims, retry scheduling, ordering):

```python
import pytest
from txoutbox.testing import StorageContract

class TestMyStorage(StorageContract):
    @pytest.fixture
    async def storage(self):
        return MyStorage(...)          # fresh, empty, per test

    async def insert(self, storage, topic, payload, key=None):
        return await storage.add(topic, payload, key=key)
```

The rules an adapter must follow are short:

1. `claim` returns due, unleased, pending rows in insertion order, leases them for `lease`, and increments
   `attempts` **at claim time** (a crash mid-batch must burn an attempt).
2. `ack` marks rows delivered: delete them or flip a status column, your call.
3. `nack` releases the lease and stores `retry_at`; `dead_letter` releases the lease and parks the row.
4. Optional but recommended, *strict ordering*: hold back a row while an earlier pending row with the same
   key is not claimable right now (leased elsewhere, or waiting for a retry). Both built-in SQL adapters do
   this with one `NOT EXISTS` clause. The relay guarantees order within a batch on its own; strict ordering
   in storage extends the guarantee across retries and across workers.

## Built-in adapters

| Adapter | Module | Notes |
| --- | --- | --- |
| `MemoryStorage`, `MemoryPublisher` | `txoutbox.adapters.memory` | Tests, demos, deterministic |
| `SqliteStorage` | `txoutbox.adapters.sqlite` | Stdlib `sqlite3`, `BEGIN IMMEDIATE`, WAL |
| `PostgresStorage` | `txoutbox.adapters.postgres` | asyncpg, `FOR UPDATE SKIP LOCKED`, extra `postgres` |

Both SQL adapters expose `insert(conn, ...)` so you write the outbox row **inside your own transaction**,
and `schema_sql()` / `create_schema()` so the table lives in your migrations.

## Configuration

```python
from datetime import timedelta
from txoutbox import Backoff, RelayConfig

RelayConfig(
    batch_size=100,                     # rows per round
    lease=timedelta(seconds=30),        # must exceed the time to publish one round
    concurrency=10,                     # ordering groups published in parallel
    max_attempts=10,                    # then dead-letter
    backoff=Backoff(base=timedelta(seconds=1), factor=2, maximum=timedelta(minutes=5), jitter=0.25),
    publish_timeout=None,               # seconds per publish, None = no limit
    poll_min=0.05, poll_max=5.0, poll_factor=2.0,
    storage_error_delay=1.0,            # sleep after a storage exception
)
```

Three ways to run:

```python
await relay.run(handle_signals=True)     # foreground loop, until SIGTERM/SIGINT or relay.stop()

async with Relay(storage, publisher):    # background task for the lifetime of the block
    await app.serve()

result = await relay.run_once()          # one round; RoundResult(claimed, published, retried, dead_lettered, blocked)
```

## Delivery semantics

* **At-least-once.** A crash between `publish` and `ack` redelivers the batch after the lease expires.
  Consumers must be idempotent; a message's `id` is a natural deduplication key.
* **Ordered per key.** Within one round, messages sharing a key are published one after another, and a
  failure blocks the rest of that key for the round (they are rescheduled together with the failed one, without
  burning an attempt). With strict-ordering storage, this holds across retries and workers too. Keyless
  messages are independent.
* **Leases bound duplicates.** Keep `lease` well above `batch_size / concurrency * publish_time`; set
  `publish_timeout` so a hanging broker cannot outlive the lease.
* **Storage bookkeeping failures are tolerated.** If `nack` itself fails, the lease expiry recovers the row.

## What txoutbox is not

Not a broker, not a task queue, not an ORM. It does not own your table and does not promise exactly-once.
Deduplication on the consumer side is a separate concern (see `inboxd`, the companion library).

## License

MIT
