"""Run: uv run python examples/sqlite_demo.py   (Ctrl-C to stop)

A producer loop inserts orders + outbox rows in one transaction every second,
while a relay publishes them to stdout. Kill it and restart it: nothing is lost,
nothing is reordered.
"""

import asyncio
import json
import logging
import random
import sqlite3

from txoutbox import Hooks, OutboxMessage, Relay, RelayConfig
from txoutbox.adapters.sqlite import SqliteStorage

DB = "demo.db"


class FlakyStdoutPublisher:
    """Fails 20% of the time so you can watch retries and backoff."""

    async def publish(self, message: OutboxMessage) -> None:
        if random.random() < 0.2:
            raise ConnectionError("broker hiccup")
        print(f"-> {message.topic} key={message.key} attempt={message.attempts} {message.payload.decode()}")


class LogHooks(Hooks):
    async def on_retry(self, message, error, retry_at):
        print(f"!! retry {message.id} at {retry_at:%H:%M:%S}: {error}")


async def producer(storage: SqliteStorage) -> None:
    conn = sqlite3.connect(DB)
    conn.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, total INTEGER)")
    while True:
        with conn:  # business row + outbox row commit together
            cur = conn.execute("INSERT INTO orders (total) VALUES (?)", (random.randint(1, 100),))
            order_id = cur.lastrowid
            storage.insert(
                conn, "orders.created", json.dumps({"order_id": order_id}).encode(), key=f"order-{order_id}"
            )
        await asyncio.sleep(1)


async def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    storage = SqliteStorage(DB)
    storage.create_schema()
    relay = Relay(
        storage,
        FlakyStdoutPublisher(),
        config=RelayConfig(batch_size=10, poll_max=1.0),
        hooks=LogHooks(),
    )
    producer_task = asyncio.create_task(producer(storage))
    try:
        await relay.run(handle_signals=True)
    finally:
        producer_task.cancel()
        storage.close()


if __name__ == "__main__":
    asyncio.run(main())
