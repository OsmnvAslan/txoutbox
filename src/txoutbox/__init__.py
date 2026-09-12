"""txoutbox: a storage-agnostic Transactional Outbox relay for asyncio.

Bring your own table, bring your own broker. Implement :class:`Storage` over your
database and :class:`Publisher` over your transport; :class:`Relay` does the rest.
"""

from .backoff import Backoff
from .message import MessageId, OutboxMessage
from .poller import AdaptivePoller
from .protocols import Hooks, Publisher, Storage
from .relay import Relay, RelayConfig, RoundResult, default_worker_id

__all__ = [
    "AdaptivePoller",
    "Backoff",
    "Hooks",
    "MessageId",
    "OutboxMessage",
    "Publisher",
    "Relay",
    "RelayConfig",
    "RoundResult",
    "Storage",
    "default_worker_id",
]

__version__ = "0.1.0"
