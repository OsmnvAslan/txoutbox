"""txoutbox: a storage-agnostic Transactional Outbox relay for asyncio.

Bring your own table, bring your own broker. Implement :class:`Storage` over your
database and :class:`Publisher` over your transport; :class:`Relay` does the rest.
"""

from importlib.metadata import PackageNotFoundError, version

from .backoff import Backoff
from .message import MessageId, OutboxMessage, OutboxStats, Payload, encode_payload
from .poller import AdaptivePoller
from .protocols import Hooks, Publisher, PublishFn, StatsProvider, Storage
from .relay import Relay, RelayConfig, RoundResult, default_worker_id

__all__ = [
    "AdaptivePoller",
    "Backoff",
    "Hooks",
    "MessageId",
    "OutboxMessage",
    "OutboxStats",
    "Payload",
    "PublishFn",
    "Publisher",
    "Relay",
    "RelayConfig",
    "RoundResult",
    "StatsProvider",
    "Storage",
    "default_worker_id",
    "encode_payload",
]

try:
    __version__ = version("txoutbox")
except PackageNotFoundError:  # pragma: no cover - source checkout without install
    __version__ = "0.0.0"
