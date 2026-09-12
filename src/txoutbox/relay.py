"""The relay: claim, publish, ack. Everything else is bookkeeping."""

from __future__ import annotations

import asyncio
import logging
import math
import os
import signal
import socket
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Self

from .backoff import Backoff
from .message import MessageId, OutboxMessage
from .poller import AdaptivePoller
from .protocols import Hooks, Publisher, PublishFn, Storage

log = logging.getLogger("txoutbox")


@dataclass(frozen=True, slots=True, kw_only=True)
class RelayConfig:
    #: Max messages claimed per round.
    batch_size: int = 50
    #: How long a claim is held. Must exceed the worst-case round:
    #: ``ceil(batch_size / concurrency) * publish_timeout``. With the defaults that is 25 s.
    lease: timedelta = timedelta(seconds=60)
    #: Max ordering groups published concurrently. Messages within a group are sequential.
    concurrency: int = 10
    #: A message that fails on its ``max_attempts``-th claim is dead-lettered.
    max_attempts: int = 10
    backoff: Backoff = field(default_factory=Backoff)
    #: Per-message publish timeout in seconds, so a hanging broker cannot outlive the
    #: lease. ``None`` disables it (and disables the worst-case round check).
    publish_timeout: float | None = 5.0
    #: Idle polling: sleep grows from ``poll_min`` to ``poll_max`` while the table is empty.
    poll_min: float = 0.05
    poll_max: float = 5.0
    poll_factor: float = 2.0
    #: Sleep after a storage error before trying again.
    storage_error_delay: float = 1.0

    def __post_init__(self) -> None:
        if self.batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if self.concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.lease <= timedelta(0):
            raise ValueError("lease must be positive")
        if self.publish_timeout is not None and self.publish_timeout <= 0:
            raise ValueError("publish_timeout must be positive or None")
        if self.storage_error_delay < 0:
            raise ValueError("storage_error_delay must be >= 0")
        worst = self.worst_case_round
        if worst is not None and worst > self.lease.total_seconds():
            log.warning(
                "RelayConfig: worst-case round ceil(%d / %d) * %.1fs = %.0fs exceeds the %.0fs "
                "lease; other workers may re-claim messages mid-round. Raise lease or "
                "concurrency, or lower batch_size or publish_timeout.",
                self.batch_size,
                self.concurrency,
                self.publish_timeout,
                worst,
                self.lease.total_seconds(),
            )

    @property
    def worst_case_round(self) -> float | None:
        """Seconds a round can take if every publish hits ``publish_timeout``."""
        if self.publish_timeout is None:
            return None
        return math.ceil(self.batch_size / self.concurrency) * self.publish_timeout


@dataclass(slots=True)
class RoundResult:
    """What happened in one :meth:`Relay.run_once`."""

    claimed: int = 0
    published: int = 0
    retried: int = 0
    dead_lettered: int = 0
    blocked: int = 0

    @property
    def failed(self) -> int:
        return self.retried + self.dead_lettered


def default_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


class _FnPublisher:
    __slots__ = ("_fn",)

    def __init__(self, fn: PublishFn) -> None:
        self._fn = fn

    async def publish(self, message: OutboxMessage) -> None:
        await self._fn(message)


class Relay:
    """Moves messages from a :class:`Storage` to a :class:`Publisher`.

    ``publisher`` is any object with ``async def publish(message)``, or such a function.

    Use it as a long-running loop (:meth:`run`), as a background task
    (``async with Relay(...)``), or one round at a time (:meth:`run_once`),
    which is handy in tests and in cron-style workers.
    """

    def __init__(
        self,
        storage: Storage,
        publisher: Publisher | PublishFn,
        *,
        config: RelayConfig | None = None,
        hooks: Hooks | None = None,
        worker_id: str | None = None,
    ) -> None:
        self.storage = storage
        self.publisher: Publisher = (
            publisher if isinstance(publisher, Publisher) else _FnPublisher(publisher)
        )
        self.config = config or RelayConfig()
        self.hooks = hooks or Hooks()
        self.worker_id = worker_id or default_worker_id()
        self._poller = AdaptivePoller(
            minimum=self.config.poll_min,
            maximum=self.config.poll_max,
            factor=self.config.poll_factor,
        )
        self._stopping = False
        self._task: asyncio.Task[None] | None = None

    # -- lifecycle ---------------------------------------------------------------------

    def wake(self) -> None:
        """Cut the idle sleep short. Call it after inserting rows, or from LISTEN/NOTIFY.

        Not thread-safe: from another thread use ``loop.call_soon_threadsafe(relay.wake)``.
        """
        self._poller.wake()

    def stop(self) -> None:
        """Ask :meth:`run` to return after the current round. Safe to call from a signal handler."""
        self._stopping = True
        self._poller.wake()

    async def run(self, *, handle_signals: bool = False) -> None:
        """Loop until :meth:`stop` is called.

        With ``handle_signals=True``, the first SIGINT/SIGTERM calls :meth:`stop`, so the
        round in flight finishes and leases are released cleanly. A second signal cancels
        the round and ``run`` returns quietly.

        ``handle_signals`` is for a process the relay owns (a dedicated worker). It replaces
        the loop's signal handlers; plain ``signal.signal`` handlers are put back on exit,
        but handlers another component registered with ``loop.add_signal_handler`` (uvicorn,
        hypercorn) cannot be restored. Inside a web app use ``async with Relay(...)`` or call
        :meth:`stop` from the framework's shutdown hook instead.
        """
        self._stopping = False
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        previous: dict[signal.Signals, object] = {}
        signals_seen = 0
        cancelled_by_signal = False

        def on_signal() -> None:
            nonlocal signals_seen, cancelled_by_signal
            signals_seen += 1
            if signals_seen == 1:
                log.info("signal received; finishing the current round")
                self.stop()
            elif task is not None and not cancelled_by_signal:
                log.warning("second signal; cancelling the current round")
                cancelled_by_signal = True
                task.cancel()

        if handle_signals:
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    previous[sig] = signal.getsignal(sig)
                    loop.add_signal_handler(sig, on_signal)
                except (NotImplementedError, RuntimeError):  # pragma: no cover - Windows
                    log.warning("cannot install handler for %s", sig.name)
        try:
            while not self._stopping:
                try:
                    result = await self.run_once()
                except asyncio.CancelledError:
                    if cancelled_by_signal and task is not None:
                        task.uncancel()
                        log.warning("round cancelled by signal; exiting")
                        return
                    raise
                except Exception:
                    delay = self.config.storage_error_delay
                    log.exception("round failed; retrying in %.1fs", delay)
                    await asyncio.sleep(delay)
                    continue
                if self._stopping:
                    break
                if result.claimed:
                    self._poller.busy()
                    if result.claimed >= self.config.batch_size:
                        await asyncio.sleep(0)  # full batch: there is probably more
                        continue
                else:
                    self._poller.idle()
                await self._poller.wait()
        finally:
            for sig, handler in previous.items():
                loop.remove_signal_handler(sig)
                if _is_plain_handler(handler):
                    signal.signal(sig, handler)  # type: ignore[arg-type]

    async def __aenter__(self) -> Self:
        self._task = asyncio.create_task(self.run(), name=f"txoutbox-relay-{self.worker_id}")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.stop()
        if self._task is not None:
            await self._task
            self._task = None

    # -- one round ---------------------------------------------------------------------

    async def run_once(self) -> RoundResult:
        """Claim one batch, publish it, report back to storage. Storage errors propagate.

        Messages the publisher accepted are acked even if the round is cancelled.
        """
        cfg = self.config
        started = time.monotonic()
        messages = await self.storage.claim(
            batch_size=cfg.batch_size, lease=cfg.lease, worker_id=self.worker_id
        )
        await self._hook(self.hooks.on_claimed, messages)
        result = RoundResult(claimed=len(messages))
        if not messages:
            return result

        acked: list[MessageId] = []
        semaphore = asyncio.Semaphore(cfg.concurrency)

        async def run_group(group: list[OutboxMessage]) -> None:
            async with semaphore:
                await self._publish_group(group, acked, result)

        try:
            async with asyncio.TaskGroup() as tg:
                for group in _group_by_key(messages):
                    tg.create_task(run_group(group))
        finally:
            if acked:
                await asyncio.shield(self.storage.ack(acked, worker_id=self.worker_id))
            duration = time.monotonic() - started
            if duration > cfg.lease.total_seconds():
                log.warning(
                    "round took %.1fs, longer than the %.0fs lease; other workers may have "
                    "re-claimed messages. Lower batch_size or raise lease/concurrency.",
                    duration,
                    cfg.lease.total_seconds(),
                )
            if result.failed:
                log.warning(
                    "round: %d claimed, %d published, %d retried, %d dead-lettered, %d blocked",
                    result.claimed,
                    result.published,
                    result.retried,
                    result.dead_lettered,
                    result.blocked,
                )
            await self._hook(self.hooks.on_round, result, duration)
        return result

    async def _publish_group(
        self, group: list[OutboxMessage], acked: list[MessageId], result: RoundResult
    ) -> None:
        for index, message in enumerate(group):
            try:
                await self._publish_one(message)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                retry_at = await self._handle_failure(message, error, result)
                await self._block_rest(group[index + 1 :], message, retry_at, result)
                return
            acked.append(message.id)
            result.published += 1
            await self._hook(self.hooks.on_published, message)

    async def _publish_one(self, message: OutboxMessage) -> None:
        if self.config.publish_timeout is None:
            await self.publisher.publish(message)
        else:
            async with asyncio.timeout(self.config.publish_timeout):
                await self.publisher.publish(message)

    async def _handle_failure(
        self, message: OutboxMessage, error: BaseException, result: RoundResult
    ) -> datetime:
        """Nack or dead-letter. Returns when the message will be eligible again."""
        text = f"{type(error).__name__}: {error}"
        now = datetime.now(UTC)
        if message.attempts >= self.config.max_attempts:
            log.error("dead-lettering %r after %d attempts: %s", message.id, message.attempts, text)
            await self._report(
                self.storage.dead_letter(message.id, worker_id=self.worker_id, error=text)
            )
            result.dead_lettered += 1
            await self._hook(self.hooks.on_dead_letter, message, error)
            return now
        retry_at = now + self.config.backoff.delay(message.attempts)
        log.log(
            logging.INFO if message.attempts == 1 else logging.DEBUG,
            "publish failed for %r (attempt %d, retry at %s): %s",
            message.id,
            message.attempts,
            retry_at.isoformat(timespec="seconds"),
            text,
        )
        await self._report(
            self.storage.nack(message.id, worker_id=self.worker_id, error=text, retry_at=retry_at)
        )
        result.retried += 1
        await self._hook(self.hooks.on_retry, message, error, retry_at)
        return retry_at

    async def _block_rest(
        self,
        rest: Sequence[OutboxMessage],
        failed: OutboxMessage,
        retry_at: datetime,
        result: RoundResult,
    ) -> None:
        """Later messages with the same key must not overtake the failed one.

        They were never tried, so they are released without burning an attempt.
        """
        if not rest:
            return
        await self._report(
            self.storage.release([m.id for m in rest], worker_id=self.worker_id, retry_at=retry_at)
        )
        result.blocked += len(rest)
        for message in rest:
            await self._hook(self.hooks.on_blocked, message, failed)

    # -- helpers -----------------------------------------------------------------------

    @staticmethod
    async def _report(call: Awaitable[None]) -> None:
        """Storage bookkeeping failures must not kill the round; the lease will expire."""
        try:
            await call
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("storage bookkeeping failed; lease expiry will recover the message")

    @staticmethod
    async def _hook(callback: Callable[..., Awaitable[None]], *args: object) -> None:
        try:
            await callback(*args)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("hook %s raised", getattr(callback, "__name__", callback))


def _is_plain_handler(handler: object) -> bool:
    """A ``signal.signal`` handler we can put back. asyncio's internal C-level stub is not."""
    if handler in (signal.SIG_DFL, signal.SIG_IGN):
        return True
    return callable(handler) and not getattr(handler, "__module__", "").startswith("asyncio")


def _group_by_key(messages: Sequence[OutboxMessage]) -> list[list[OutboxMessage]]:
    """Messages sharing a key form one ordered group; keyless messages are singletons."""
    groups: dict[str, list[OutboxMessage]] = {}
    order: list[list[OutboxMessage]] = []
    for message in messages:
        if message.key is None:
            order.append([message])
            continue
        group = groups.get(message.key)
        if group is None:
            group = groups[message.key] = []
            order.append(group)
        group.append(message)
    return order
