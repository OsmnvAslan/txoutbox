import asyncio
import signal
import time
from datetime import UTC, datetime, timedelta

import pytest

from txoutbox import Backoff, Hooks, OutboxMessage, Relay, RelayConfig
from txoutbox.adapters.memory import MemoryPublisher, MemoryStorage, Status


def cfg(**kw) -> RelayConfig:
    base = dict(poll_min=0.01, poll_max=0.05, backoff=Backoff(base=timedelta(seconds=10), jitter=0))
    base.update(kw)
    return RelayConfig(**base)


async def test_publishes_everything_and_acks() -> None:
    storage, publisher = MemoryStorage(), MemoryPublisher()
    for i in range(7):
        storage.add("orders", f"{i}".encode())
    relay = Relay(storage, publisher, config=cfg(batch_size=3))
    results = [await relay.run_once() for _ in range(4)]
    assert [r.claimed for r in results] == [3, 3, 1, 0]
    assert [m.payload for m in publisher.published] == [f"{i}".encode() for i in range(7)]
    assert all(r.status is Status.DONE for r in storage.records())


async def test_same_key_is_sequential_and_different_keys_overlap() -> None:
    storage = MemoryStorage()
    active: dict[str | None, int] = {}
    max_active_total = 0
    overlap_within_key = False

    class Pub:
        async def publish(self, m: OutboxMessage) -> None:
            nonlocal max_active_total, overlap_within_key
            if active.get(m.key, 0):
                overlap_within_key = True
            active[m.key] = active.get(m.key, 0) + 1
            max_active_total = max(max_active_total, sum(active.values()))
            await asyncio.sleep(0.01)
            active[m.key] -= 1

    for i in range(12):
        storage.add("t", str(i).encode(), key=f"k{i % 3}")
    result = await Relay(storage, Pub(), config=cfg(concurrency=10)).run_once()
    assert result.published == 12
    assert not overlap_within_key
    assert max_active_total == 3


async def test_concurrency_limit() -> None:
    storage = MemoryStorage()
    active = max_active = 0

    class Pub:
        async def publish(self, m: OutboxMessage) -> None:
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.005)
            active -= 1

    for _ in range(10):
        storage.add("t", b"")
    await Relay(storage, Pub(), config=cfg(concurrency=2)).run_once()
    assert max_active == 2


async def test_failure_is_nacked_with_backoff_and_hook() -> None:
    storage = MemoryStorage()
    publisher = MemoryPublisher(fail=lambda m: RuntimeError("broker down"))
    seen: list[tuple[OutboxMessage, str, datetime]] = []

    class H(Hooks):
        async def on_retry(self, message, error, retry_at):
            seen.append((message, str(error), retry_at))

    mid = storage.add("t", b"x")
    before = datetime.now(UTC)
    result = await Relay(storage, publisher, config=cfg(), hooks=H()).run_once()
    assert (result.retried, result.published) == (1, 0)
    record = storage.get(mid)
    assert record.status is Status.PENDING
    assert record.last_error == "RuntimeError: broker down"
    assert record.retry_at is not None and record.retry_at >= before + timedelta(seconds=10)
    assert record.leased_by is None
    assert len(seen) == 1 and seen[0][1] == "broker down"
    # not due yet
    assert (await Relay(storage, publisher, config=cfg()).run_once()).claimed == 0


async def test_dead_letter_after_max_attempts() -> None:
    storage = MemoryStorage()
    publisher = MemoryPublisher(fail=lambda m: ValueError("nope"))
    dead: list[OutboxMessage] = []

    class H(Hooks):
        async def on_dead_letter(self, message, error):
            dead.append(message)

    mid = storage.add("t", b"x")
    config = cfg(max_attempts=3, backoff=Backoff(base=timedelta(microseconds=1), jitter=0))
    relay = Relay(storage, publisher, config=config, hooks=H())
    outcomes = []
    for _ in range(3):
        await asyncio.sleep(0.001)
        outcomes.append(await relay.run_once())
    assert [r.retried for r in outcomes] == [1, 1, 0]
    assert [r.dead_lettered for r in outcomes] == [0, 0, 1]
    assert storage.get(mid).status is Status.DEAD
    assert storage.get(mid).attempts == 3
    assert [m.id for m in dead] == [mid]


async def test_failure_blocks_rest_of_key_group_but_not_other_keys() -> None:
    storage = MemoryStorage()
    ids = [storage.add("t", str(i).encode(), key="k") for i in range(3)]
    other = storage.add("t", b"o", key="other")
    publisher = MemoryPublisher(fail=lambda m: RuntimeError("x") if m.id == ids[0] else None)
    blocked: list[tuple] = []

    class H(Hooks):
        async def on_blocked(self, message, blocked_by):
            blocked.append((message.id, blocked_by.id))

    result = await Relay(storage, publisher, config=cfg(), hooks=H()).run_once()
    assert (result.claimed, result.published, result.retried, result.blocked) == (4, 1, 1, 2)
    assert [m.id for m in publisher.published] == [other]
    assert blocked == [(ids[1], ids[0]), (ids[2], ids[0])]
    for i in ids[1:]:
        rec = storage.get(i)
        assert rec.last_error == f"blocked by {ids[0]!r}"
        assert rec.retry_at == storage.get(ids[0]).retry_at


async def test_publish_timeout_counts_as_failure() -> None:
    storage = MemoryStorage()
    storage.add("t", b"x")
    publisher = MemoryPublisher(delay=0.2)
    result = await Relay(storage, publisher, config=cfg(publish_timeout=0.01)).run_once()
    assert result.retried == 1
    assert storage.records()[0].last_error.startswith("TimeoutError")


async def test_run_loop_stops_gracefully_and_drains() -> None:
    storage, publisher = MemoryStorage(), MemoryPublisher(delay=0.01)
    for _ in range(5):
        storage.add("t", b"x")
    relay = Relay(storage, publisher, config=cfg(batch_size=2))
    task = asyncio.create_task(relay.run())
    while len(publisher.published) < 5:
        await asyncio.sleep(0.005)
    relay.stop()
    await asyncio.wait_for(task, 1)
    assert len(publisher.published) == 5


async def test_context_manager_and_wake() -> None:
    storage, publisher = MemoryStorage(), MemoryPublisher()
    async with Relay(storage, publisher, config=cfg(poll_min=5, poll_max=5)) as relay:
        await asyncio.sleep(0.02)  # relay is now asleep for 5s
        storage.add("t", b"x")
        relay.wake()
        deadline = time.monotonic() + 1
        while not publisher.published and time.monotonic() < deadline:
            await asyncio.sleep(0.005)
    assert len(publisher.published) == 1


async def test_storage_errors_do_not_kill_the_loop() -> None:
    calls = 0

    class Flaky(MemoryStorage):
        async def claim(self, **kw):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ConnectionError("db hiccup")
            return await super().claim(**kw)

    storage, publisher = Flaky(), MemoryPublisher()
    storage.add("t", b"x")
    relay = Relay(storage, publisher, config=cfg(storage_error_delay=0.01))
    task = asyncio.create_task(relay.run())
    deadline = time.monotonic() + 1
    while not publisher.published and time.monotonic() < deadline:
        await asyncio.sleep(0.005)
    relay.stop()
    await task
    assert len(publisher.published) == 1 and calls >= 2


async def test_bookkeeping_errors_do_not_kill_the_round() -> None:
    class Broken(MemoryStorage):
        async def nack(self, *a, **kw):
            raise ConnectionError("db gone")

    storage = Broken()
    storage.add("t", b"a")
    storage.add("t", b"b")
    publisher = MemoryPublisher(fail=lambda m: RuntimeError() if m.payload == b"a" else None)
    result = await Relay(storage, publisher, config=cfg()).run_once()
    assert (result.published, result.retried) == (1, 1)


async def test_hook_exceptions_are_swallowed() -> None:
    class Bad(Hooks):
        async def on_published(self, message):
            raise RuntimeError("telemetry down")

    storage, publisher = MemoryStorage(), MemoryPublisher()
    storage.add("t", b"x")
    result = await Relay(storage, publisher, config=cfg(), hooks=Bad()).run_once()
    assert result.published == 1


async def test_signal_handler_stops_run() -> None:
    storage, publisher = MemoryStorage(), MemoryPublisher()
    relay = Relay(storage, publisher, config=cfg())
    task = asyncio.create_task(relay.run(handle_signals=True))
    await asyncio.sleep(0.02)
    signal.raise_signal(signal.SIGTERM)
    await asyncio.wait_for(task, 1)


async def test_cancellation_propagates() -> None:
    storage = MemoryStorage()
    storage.add("t", b"x")
    relay = Relay(storage, MemoryPublisher(delay=10), config=cfg())
    task = asyncio.create_task(relay.run())
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_config_validation() -> None:
    for bad in (
        dict(batch_size=0),
        dict(concurrency=0),
        dict(max_attempts=0),
        dict(lease=timedelta(0)),
    ):
        with pytest.raises(ValueError):
            RelayConfig(**bad)


def test_worker_id_is_unique() -> None:
    a, b = Relay(MemoryStorage(), MemoryPublisher()), Relay(MemoryStorage(), MemoryPublisher())
    assert a.worker_id != b.worker_id
