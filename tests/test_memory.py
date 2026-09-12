import pytest

from txoutbox.adapters.memory import MemoryStorage
from txoutbox.testing import StorageContract


class TestMemoryStorage(StorageContract):
    @pytest.fixture
    def storage(self) -> MemoryStorage:
        return MemoryStorage()

    async def insert(self, storage, topic, payload, key=None):
        return await storage.add(topic, payload, key=key)


class TestMemoryStorageLoose(TestMemoryStorage):
    strict_ordering = False

    @pytest.fixture
    def storage(self) -> MemoryStorage:
        return MemoryStorage(strict_ordering=False)


async def test_dict_payload_round_trip() -> None:
    storage = MemoryStorage()
    await storage.add("t", {"order_id": 7, "note": "привет"})
    (m,) = await storage.claim(
        batch_size=1, lease=__import__("datetime").timedelta(seconds=1), worker_id="w"
    )
    assert m.json() == {"order_id": 7, "note": "привет"}
    assert m.text() == '{"order_id":7,"note":"привет"}'
