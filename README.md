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

Only the relay. You give it a storage (four methods over your table) and a publisher (one function),
and it handles the hard parts:

| Concern | How |
| --- | --- |
| Concurrent workers | Leases: `claim()` hands each row to one worker for a limited time. Every write back is fenced by `worker_id`, so a worker whose lease expired cannot touch a row someone else re-claimed |
| Crashed workers | Leases expire, the row is claimed again, the attempt is counted |
| Ordering | Messages sharing a `key` are published sequentially, in insertion order; different keys run in parallel. A failure holds back the rest of its key without burning their attempts |
| Retries | Exponential backoff with jitter, per message, scheduled in storage |
| Poison messages | Dead-lettered after `max_attempts`, followers of a dead head carry on |
| Idle polling | Adaptive: sleep grows from 50 ms to 5 s while the table is empty, resets on work; `wake()` for LISTEN/NOTIFY |
| Broker hangs | Per-message `publish_timeout` (10 s by default) so a hang cannot outlive the lease |
| Shutdown | First `SIGTERM`/`SIGINT` finishes the round in flight, second one cancels it. Published messages are acked even on cancel |
| Observability | `Hooks` for every outcome, `stats()` with queue depth and oldest-pending age for alerting |

## Install

```bash
pip install "txoutbox[postgres]"   # asyncpg adapter
pip install txoutbox               # core only, for your own adapter
```

## Quick start (Postgres)

```python
import asyncio
import asyncpg
from txoutbox import Relay, OutboxMessage
from txoutbox.adapters.postgres import PostgresStorage

async def main() -> None:
    pool = await asyncpg.create_pool("postgresql://app@localhost/app")
    outbox = PostgresStorage(pool)
    await outbox.create_schema()        # or paste outbox.schema_sql() into your migrations

    # Producer side: the business row and the event commit together.
    async with pool.acquire() as conn, conn.transaction():
        order_id = await conn.fetchval("INSERT INTO orders (total) VALUES ($1) RETURNING id", 42)
        await outbox.insert(conn, "orders.created", {"order_id": order_id}, key=f"order-{order_id}")

    # Relay side: the publisher is one function. Kafka, NSQ, HTTP, whatever you use.
    async def publish(message: OutboxMessage) -> None:
        print(message.topic, message.key, message.json())

    await Relay(outbox, publish).run(handle_signals=True)

asyncio.run(main())
```

Run the relay in as many processes as you like. Run it inside your web app with
`async with Relay(outbox, publish): ...`, or one round at a time from a cron job with `await relay.run_once()`.

### Not polling: LISTEN/NOTIFY

```python
outbox = PostgresStorage(pool, notify_channel="outbox")   # schema_sql() now includes the trigger
relay = Relay(outbox, publish)
listener = await outbox.listen(relay.wake)                # new row -> relay wakes up immediately
```

### Kafka in three lines

```python
from aiokafka import AIOKafkaProducer

producer = AIOKafkaProducer(bootstrap_servers="kafka:9092")

async def publish(message: OutboxMessage) -> None:
    await producer.send_and_wait(
        message.topic, message.payload, key=message.key.encode() if message.key else None
    )
```

## Your own storage

`PostgresStorage` is a convenience. The point of txoutbox is that the relay does not care where the rows live.
A storage is four fenced write-backs plus `claim`:

```python
class Storage(Protocol):
    async def claim(self, *, batch_size: int, lease: timedelta, worker_id: str) -> Sequence[OutboxMessage]: ...
    async def ack(self, ids: Sequence[MessageId], *, worker_id: str) -> None: ...
    async def nack(self, message_id: MessageId, *, worker_id: str, error: str, retry_at: datetime) -> None: ...
    async def release(self, ids: Sequence[MessageId], *, worker_id: str, retry_at: datetime) -> None: ...
    async def dead_letter(self, message_id: MessageId, *, worker_id: str, error: str) -> None: ...
```

The rules:

1. `claim` returns due, unleased, pending rows in insertion order, leases them for `lease` on behalf of
   `worker_id`, and increments `attempts` **at claim time**. A crash mid-batch must burn an attempt.
2. Every other method only touches rows **currently leased by `worker_id`**. Ignore the rest.
3. `ack` marks rows delivered. `nack` records the error and schedules `retry_at`. `release` gives rows back
   with `attempts - 1` and no error: the relay never tried them. `dead_letter` parks the row.
4. Optional but recommended, *strict ordering*: hold back a row while an earlier pending row with the same
   key is not claimable right now. The built-in adapters do this with one `NOT EXISTS` clause and, on
   Postgres, a transaction-scoped advisory lock so concurrent workers see committed state.

Then let the contract tests find what you got wrong. They cover lease expiry, fencing, concurrent claims,
retry scheduling and ordering under a race:

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

A complete Django ORM storage is in [`examples/django_outbox/`](examples/django_outbox/): a model, the five methods with
`sync_to_async`, and `select_for_update(skip_locked=True)`.

## Built-in adapters

| Adapter | Module | Notes |
| --- | --- | --- |
| `PostgresStorage` | `txoutbox.adapters.postgres` | asyncpg, `FOR UPDATE SKIP LOCKED`, advisory lock for strict ordering, LISTEN/NOTIFY, `visibility_delay`, `purge()`, `stats()` |
| `SqliteStorage` | `txoutbox.adapters.sqlite` | Stdlib `sqlite3`. Tests, tooling, single-process apps |
| `MemoryStorage`, `MemoryPublisher` | `txoutbox.adapters.memory` | Deterministic in-process doubles for your test suite |

SQL adapters expose `insert(conn, ...)` so the outbox row goes into **your** transaction, `schema_sql()` for
your migration tool, `stats()` for dashboards, and `purge(older_than)` to keep the table small.

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
    publish_timeout=10.0,               # seconds per publish, None = no limit
    poll_min=0.05, poll_max=5.0, poll_factor=2.0,
    storage_error_delay=1.0,            # sleep after a storage exception
)
```

## Delivery semantics

* **At-least-once.** A crash between `publish` and `ack` redelivers the batch after the lease expires.
  Consumers must be idempotent; a message's `id` is a natural deduplication key.
* **Ordered per key.** Messages sharing a key are published one after another. A failure holds back the
  rest of that key: they are released untried, with the same retry time and no attempt burned. With
  strict-ordering storage this holds across retries and across workers. Keyless messages are independent.
* **Insertion order is id order, not commit order.** On Postgres a `BIGSERIAL` id is assigned at `INSERT`;
  a transaction with a smaller id can commit later and be picked up after a larger one. If that matters
  for your keys, set `PostgresStorage(visibility_delay=timedelta(seconds=2))` to only claim rows older than
  your longest producer transaction.
* **Leases bound duplicates.** The relay logs a warning when a round outlives its lease. Keep `lease` well
  above `batch_size / concurrency * publish_time` and leave `publish_timeout` on.
* **Bookkeeping failures are tolerated.** If `nack` itself fails, lease expiry recovers the row.

## What txoutbox is not

Not a broker, not a task queue, not an ORM. It does not own your table and does not promise exactly-once.
Deduplication on the consumer side is a separate concern (see `inboxd`, the companion library).

## License

MIT
