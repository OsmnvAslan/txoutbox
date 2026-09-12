"""Retry delay policy."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True, slots=True, kw_only=True)
class Backoff:
    """Exponential backoff with proportional jitter.

    Delay for attempt ``n`` (1-based) is ``min(base * factor ** (n - 1), maximum)``,
    multiplied by a random factor in ``[1 - jitter, 1 + jitter]``. With the defaults:
    1 s, 2 s, 4 s, ... capped at 5 min, each within ±25 %.
    """

    base: timedelta = timedelta(seconds=1)
    factor: float = 2.0
    maximum: timedelta = timedelta(minutes=5)
    jitter: float = 0.25

    def __post_init__(self) -> None:
        if self.factor < 1:
            raise ValueError("factor must be >= 1")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be within [0, 1]")
        if self.base <= timedelta(0):
            raise ValueError("base must be positive")
        if self.maximum < self.base:
            raise ValueError("maximum must be >= base")

    def delay(self, attempt: int) -> timedelta:
        if attempt < 1:
            raise ValueError("attempt is 1-based")
        cap = self.maximum.total_seconds()
        try:
            raw = self.base.total_seconds() * self.factor ** (attempt - 1)
        except OverflowError:
            raw = cap
        capped = min(raw, cap)
        if self.jitter:
            capped *= random.uniform(1 - self.jitter, 1 + self.jitter)
        return timedelta(seconds=capped)
