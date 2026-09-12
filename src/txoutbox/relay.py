"""The relay: claim, publish, ack. Everything else is bookkeeping."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Self

from .backoff import Backoff
from .message import MessageId, OutboxMessage
from .poller import AdaptivePoller
from .protocols import Hooks, Publisher, Storage

log = logging.getLogger("txoutbox")


@dataclass(frozen=True, slots=True, kw_only=True)
class RelayConfig:
    #: Max messages claimed per round.
    batch_size: int = 100
    #: How long a claim is held. Must comfortably exceed the time to publish one round.
    lease: timedelta = timedelta(seconds=30)
    #: Max ordering groups published concurrently. Messages within a group are sequential.
    concurrency: int = 10
    #: A message that fails on its ``max_attempts``-th claim is dead-lettered.
    max_attempts: int = 10
    backoff: Backoff = field(default_factory=Backoff)
    #: Per-message publish timeout in seconds. ``None`` disables it.
    publish_timeout: float | None = None
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


class Relay:
    """Moves messages from a :class:`Storage` to a :class:`Publisher`.

    Use it as a long-running loop (:meth:`run`), as a background task
    (``async with Relay(...)``), or one round at a time (:meth:`run_once`),
    which is handy in tests and in cron-style workers.
    """

    def __init__(
        self,
        storage: Storage,
        publisher: Publisher,
        *,
        config: RelayConfig | None = None,
        hooks: Hooks | None = None,
        worker_id: str | None = None,
    ) -> None:
        self.storage = storage
        self.publisher = publisher
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
        """Cut the idle sleep short. Call it after inserting rows, or from LISTEN/NOTIFY."""
        self._poller.wake()

    def stop(self) -> None:
        """Ask :meth:`run` to return after the current round. Safe to call from a signal handler."""
        self._stopping = True
        self._poller.wake()

    async def run(self, *, handle_signals: bool = False) -> None:
        """Loop until :meth:`stop` is called.

        With ``handle_signals=True``, SIGINT and SIGTERM call :meth:`stop`, so the
        round in flight finishes and leases are released cleanly.
        """
        self._stopping = False
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        if handle_signals:
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, self.stop)
                    installed.append(sig)
                except (NotImplementedError, RuntimeError):  # pragma: no cover - Windows
                    log.warning("cannot install handler for %s", sig.name)
        try:
            while not self._stopping:
                try:
                    result = await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    delay = self.config.storage_error_delay
                    log.exception("storage error; retrying in %.1fs", delay)
                    await asyncio.sleep(delay)
                    continue
                if self._stopping:
                    break
                if result.claimed >= self.config.batch_size:
                    await asyncio.sleep(0)  # full batch: there is probably more, don't sleep
                    continue
                if result.claimed:
                    self._poller.busy()
                else:
                    self._poller.idle()
                await self._poller.wait()
        finally:
            for sig in installed:
                loop.remove_signal_handler(sig)

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
        """Claim one batch, publish it, report back to storage. Storage errors propagate."""
        cfg = self.config
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

        async with asyncio.TaskGroup() as tg:
            for group in _group_by_key(messages):
                tg.create_task(run_group(group))

        if acked:
            await self.storage.ack(acked)
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
            await self._report(self.storage.dead_letter(message.id, error=text))
            result.dead_lettered += 1
            await self._hook(self.hooks.on_dead_letter, message, error)
            return now
        retry_at = now + self.config.backoff.delay(message.attempts)
        log.warning("publish failed for %r (attempt %d): %s", message.id, message.attempts, text)
        await self._report(self.storage.nack(message.id, error=text, retry_at=retry_at))
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
        """Later messages with the same key must not overtake the failed one."""
        text = f"blocked by {failed.id!r}"
        for message in rest:
            await self._report(self.storage.nack(message.id, error=text, retry_at=retry_at))
            result.blocked += 1
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
