"""Adaptive polling: back off while the table is empty, snap back when work appears."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


@dataclass(kw_only=True)
class AdaptivePoller:
    """Sleeps between empty polls, growing the interval up to ``maximum``.

    * :meth:`idle` doubles (by ``factor``) the next wait, capped at ``maximum``.
    * :meth:`busy` resets the next wait to ``minimum``.
    * :meth:`wake` cuts any sleep short. Use it from a ``LISTEN/NOTIFY`` handler or
      from the code that just inserted an outbox row in the same process.
    """

    minimum: float = 0.05
    maximum: float = 5.0
    factor: float = 2.0
    _current: float = field(init=False, repr=False)
    _wakeup: asyncio.Event = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.minimum <= 0 or self.maximum < self.minimum or self.factor < 1:
            raise ValueError("require 0 < minimum <= maximum and factor >= 1")
        self._current = self.minimum
        self._wakeup = asyncio.Event()

    @property
    def current(self) -> float:
        return self._current

    def busy(self) -> None:
        self._current = self.minimum

    def idle(self) -> None:
        self._current = min(self._current * self.factor, self.maximum)

    def wake(self) -> None:
        self._wakeup.set()

    async def wait(self) -> None:
        """Sleep for the current interval, or less if :meth:`wake` is called.

        A :meth:`wake` that happened before ``wait`` was entered is not lost: the
        next ``wait`` returns immediately.
        """
        try:
            await asyncio.wait_for(self._wakeup.wait(), timeout=self._current)
        except TimeoutError:
            pass
        finally:
            self._wakeup.clear()
