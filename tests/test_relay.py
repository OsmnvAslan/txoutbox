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
        await storage.add("orders", f"{i}".encode())
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
        await storage.add("t", str(i).encode(), key=f"k{i % 3}")
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
        await storage.add("t", b"")
    await Relay(storage, Pub(), config=cfg(concurrency=2)).run_once()
    assert max_active == 2


async def test_failure_is_nacked_with_backoff_and_hook() -> None:
    storage = MemoryStorage()
    publisher = MemoryPublisher(fail=lambda m: RuntimeError("broker down"))
    seen: list[tuple[OutboxMessage, str, datetime]] = []

    class H(Hooks):
        async def on_retry(self, message, error, retry_at):
            seen.append((message, str(error), retry_at))

    mid = await storage.add("t", b"x")
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

    mid = await storage.add("t", b"x")
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


async def test_failure_releases_rest_of_key_group_without_burning_attempts() -> None:
    storage = MemoryStorage()
    ids = [await storage.add("t", str(i).encode(), key="k") for i in range(3)]
    other = await storage.add("t", b"o", key="other")
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
        assert rec.attempts == 0, "released messages must not burn an attempt"
        assert rec.last_error is None
        assert rec.retry_at == storage.get(ids[0]).retry_at
        assert rec.leased_by is None


async def test_followers_survive_a_poison_head() -> None:
    """A head that dies after max_attempts must not drag its followers down with it."""
    storage = MemoryStorage()
    head = await storage.add("t", b"head", key="k")
    f1 = await storage.add("t", b"f1", key="k")
    f2 = await storage.add("t", b"f2", key="k")
    failures = {head: 99, f1: 1}  # head always fails, f1 fails once

    def fail(m: OutboxMessage) -> BaseException | None:
        if failures.get(m.id, 0) > 0:
            failures[m.id] -= 1
            return RuntimeError("boom")
        return None

    publisher = MemoryPublisher(fail=fail)
    config = cfg(max_attempts=3, backoff=Backoff(base=timedelta(microseconds=1), jitter=0))
    relay = Relay(storage, publisher, config=config)
    for _ in range(6):
        await asyncio.sleep(0.001)
        await relay.run_once()
    assert storage.get(head).status is Status.DEAD
    assert storage.get(f1).status is Status.DONE and storage.get(f1).attempts == 2
    assert storage.get(f2).status is Status.DONE and storage.get(f2).attempts == 1
    assert storage.get(f2).last_error is None
    assert [m.payload for m in publisher.published] == [b"f1", b"f2"]


async def test_publish_timeout_counts_as_failure() -> None:
    storage = MemoryStorage()
    await storage.add("t", b"x")
    publisher = MemoryPublisher(delay=0.2)
    result = await Relay(storage, publisher, config=cfg(publish_timeout=0.01)).run_once()
    assert result.retried == 1
    assert storage.records()[0].last_error.startswith("TimeoutError")


async def test_run_loop_stops_gracefully_and_drains() -> None:
    storage, publisher = MemoryStorage(), MemoryPublisher(delay=0.01)
    for _ in range(5):
        await storage.add("t", b"x")
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
        await storage.add("t", b"x")
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
    await storage.add("t", b"x")
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
    await storage.add("t", b"a")
    await storage.add("t", b"b")
    publisher = MemoryPublisher(fail=lambda m: RuntimeError() if m.payload == b"a" else None)
    result = await Relay(storage, publisher, config=cfg()).run_once()
    assert (result.published, result.retried) == (1, 1)


async def test_hook_exceptions_are_swallowed() -> None:
    class Bad(Hooks):
        async def on_published(self, message):
            raise RuntimeError("telemetry down")

    storage, publisher = MemoryStorage(), MemoryPublisher()
    await storage.add("t", b"x")
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
    await storage.add("t", b"x")
    relay = Relay(storage, MemoryPublisher(delay=10), config=cfg())
    task = asyncio.create_task(relay.run())
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_config_validation() -> None:
    for bad in (
        dict(publish_timeout=0),
        dict(storage_error_delay=-1),
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


async def test_publisher_can_be_a_plain_function() -> None:
    storage = MemoryStorage()
    await storage.add("t", {"n": 1})
    seen: list[OutboxMessage] = []

    async def publish(message: OutboxMessage) -> None:
        seen.append(message)

    result = await Relay(storage, publish, config=cfg()).run_once()
    assert result.published == 1 and seen[0].json() == {"n": 1}


async def test_poller_resets_after_full_batches() -> None:
    storage, publisher = MemoryStorage(), MemoryPublisher()
    relay = Relay(storage, publisher, config=cfg(batch_size=2, poll_min=0.01, poll_max=1.0))
    poller = relay._poller
    for _ in range(8):
        poller.idle()
    assert poller.current == 1.0
    for _ in range(4):
        await storage.add("t", b"x")  # exactly two full batches
    task = asyncio.create_task(relay.run())
    while len(publisher.published) < 4:
        await asyncio.sleep(0.005)
    relay.stop()
    await task
    assert poller.current < 0.1, "burst of full batches must not leave the poller at maximum"


async def test_cancellation_still_acks_what_was_published() -> None:
    storage = MemoryStorage()
    fast = await storage.add("t", b"fast")
    slow = await storage.add("t", b"slow")

    class Pub:
        async def publish(self, m: OutboxMessage) -> None:
            if m.id == slow:
                await asyncio.sleep(10)

    relay = Relay(storage, Pub(), config=cfg(publish_timeout=None))
    task = asyncio.create_task(relay.run_once())
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert storage.get(fast).status is Status.DONE
    assert storage.get(slow).status is Status.PENDING


async def test_on_round_hook_and_failure_summary(caplog: pytest.LogCaptureFixture) -> None:
    storage = MemoryStorage()
    await storage.add("t", b"x")
    rounds: list[tuple[int, float]] = []

    class H(Hooks):
        async def on_round(self, result, duration):
            rounds.append((result.claimed, duration))

    publisher = MemoryPublisher(fail=lambda m: RuntimeError("down"))
    import logging

    logging.getLogger("txoutbox").setLevel(logging.WARNING)
    with caplog.at_level(logging.WARNING, logger="txoutbox"):
        await Relay(storage, publisher, config=cfg(), hooks=H()).run_once()
    assert rounds and rounds[0][0] == 1
    assert sum("round:" in r.message for r in caplog.records) == 1
    logging.getLogger("txoutbox").setLevel(logging.CRITICAL)


async def test_second_signal_cancels_the_round_and_returns_quietly() -> None:
    storage = MemoryStorage()
    await storage.add("t", b"x")
    relay = Relay(storage, MemoryPublisher(delay=10), config=cfg(publish_timeout=None))
    task = asyncio.create_task(relay.run(handle_signals=True))
    await asyncio.sleep(0.05)
    signal.raise_signal(signal.SIGTERM)
    await asyncio.sleep(0.05)
    assert not task.done()
    signal.raise_signal(signal.SIGTERM)
    await asyncio.wait_for(task, 1)  # no CancelledError leaks out
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


async def test_plain_signal_handler_is_restored() -> None:
    hits: list[int] = []
    previous = signal.signal(signal.SIGTERM, lambda signum, frame: hits.append(signum))
    try:
        relay = Relay(MemoryStorage(), MemoryPublisher(), config=cfg())
        task = asyncio.create_task(relay.run(handle_signals=True))
        await asyncio.sleep(0.02)
        signal.raise_signal(signal.SIGTERM)
        await asyncio.wait_for(task, 1)
        assert hits == []  # the relay handled it
        signal.raise_signal(signal.SIGTERM)
        assert hits == [signal.SIGTERM]  # ours is back
    finally:
        signal.signal(signal.SIGTERM, previous)


async def test_loop_signal_handler_is_not_clobbered_into_a_noop() -> None:
    """We cannot restore a loop-level handler, but we must not leave asyncio's C stub behind."""
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, lambda: None)
    try:
        relay = Relay(MemoryStorage(), MemoryPublisher(), config=cfg())
        task = asyncio.create_task(relay.run(handle_signals=True))
        await asyncio.sleep(0.02)
        signal.raise_signal(signal.SIGTERM)
        await asyncio.wait_for(task, 1)
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)


def test_config_warns_when_worst_case_round_exceeds_lease(caplog: pytest.LogCaptureFixture) -> None:
    import logging

    logging.getLogger("txoutbox").setLevel(logging.WARNING)
    try:
        with caplog.at_level(logging.WARNING, logger="txoutbox"):
            RelayConfig()  # defaults are consistent
            assert not caplog.records
            bad = RelayConfig(
                batch_size=100, concurrency=10, publish_timeout=10, lease=timedelta(seconds=30)
            )
            assert bad.worst_case_round == 100
            assert any("worst-case round" in r.message for r in caplog.records)
    finally:
        logging.getLogger("txoutbox").setLevel(logging.CRITICAL)
    assert RelayConfig(publish_timeout=None).worst_case_round is None


async def test_first_failure_is_logged_at_info(caplog: pytest.LogCaptureFixture) -> None:
    import logging

    storage = MemoryStorage()
    await storage.add("t", b"x")
    publisher = MemoryPublisher(fail=lambda m: RuntimeError("broker down"))
    relay = Relay(
        storage, publisher, config=cfg(backoff=Backoff(base=timedelta(microseconds=1), jitter=0))
    )
    logging.getLogger("txoutbox").setLevel(logging.DEBUG)
    try:
        with caplog.at_level(logging.DEBUG, logger="txoutbox"):
            await relay.run_once()
            await asyncio.sleep(0.001)
            await relay.run_once()
    finally:
        logging.getLogger("txoutbox").setLevel(logging.CRITICAL)
    levels = [r.levelno for r in caplog.records if "broker down" in r.message]
    assert levels == [logging.INFO, logging.DEBUG]


class _DownStorage(MemoryStorage):
    async def claim(self, **kw):
        raise ConnectionError("db down")


async def test_stop_cuts_the_storage_error_pause_short() -> None:
    relay = Relay(_DownStorage(), MemoryPublisher(), config=cfg(storage_error_delay=30))
    task = asyncio.create_task(relay.run())
    await asyncio.sleep(0.05)  # now sleeping in the error branch
    relay.stop()
    await asyncio.wait_for(task, 1)


async def test_two_signals_during_storage_error_pause_return_quietly() -> None:
    relay = Relay(_DownStorage(), MemoryPublisher(), config=cfg(storage_error_delay=30))
    task = asyncio.create_task(relay.run(handle_signals=True))
    await asyncio.sleep(0.05)
    signal.raise_signal(signal.SIGTERM)
    signal.raise_signal(signal.SIGTERM)
    await asyncio.wait_for(task, 1)  # no CancelledError leaks out
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


async def test_external_cancellation_still_propagates_from_the_pause() -> None:
    relay = Relay(_DownStorage(), MemoryPublisher(), config=cfg(storage_error_delay=30))
    task = asyncio.create_task(relay.run())
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
