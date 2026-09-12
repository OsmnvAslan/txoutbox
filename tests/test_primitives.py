import asyncio
import time
from datetime import timedelta

import pytest

from txoutbox import AdaptivePoller, Backoff


def test_backoff_grows_and_caps() -> None:
    b = Backoff(base=timedelta(seconds=1), factor=2, maximum=timedelta(seconds=5), jitter=0)
    assert [b.delay(n).total_seconds() for n in (1, 2, 3, 4, 5)] == [1, 2, 4, 5, 5]


def test_backoff_jitter_bounds() -> None:
    b = Backoff(base=timedelta(seconds=10), jitter=0.5)
    for _ in range(200):
        assert 5 <= b.delay(1).total_seconds() <= 15


def test_backoff_validation() -> None:
    with pytest.raises(ValueError):
        Backoff(factor=0.5)
    with pytest.raises(ValueError):
        Backoff(jitter=2)
    with pytest.raises(ValueError):
        Backoff(base=timedelta(0))
    with pytest.raises(ValueError):
        Backoff().delay(0)


async def test_poller_grows_on_idle_and_resets_on_busy() -> None:
    p = AdaptivePoller(minimum=0.01, maximum=0.05, factor=2)
    assert p.current == 0.01
    p.idle()
    p.idle()
    p.idle()
    p.idle()
    assert p.current == 0.05
    p.busy()
    assert p.current == 0.01


async def test_poller_wait_sleeps_and_wake_cuts_short() -> None:
    p = AdaptivePoller(minimum=1, maximum=1)
    start = time.monotonic()
    asyncio.get_running_loop().call_later(0.02, p.wake)
    await p.wait()
    assert time.monotonic() - start < 0.5


async def test_poller_wake_before_wait_is_not_lost() -> None:
    p = AdaptivePoller(minimum=1, maximum=1)
    p.wake()
    start = time.monotonic()
    await p.wait()
    assert time.monotonic() - start < 0.5
    # and the wake is consumed: the next wait sleeps for real
    p2 = AdaptivePoller(minimum=0.02, maximum=0.02)
    p2.wake()
    await p2.wait()
    start = time.monotonic()
    await p2.wait()
    assert time.monotonic() - start >= 0.015


def test_poller_validation() -> None:
    with pytest.raises(ValueError):
        AdaptivePoller(minimum=0)
    with pytest.raises(ValueError):
        AdaptivePoller(minimum=2, maximum=1)
