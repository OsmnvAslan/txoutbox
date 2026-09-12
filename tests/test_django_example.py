"""Runs the storage contract against the Django ORM recipe in ``examples/django_outbox``.

Needs Django and psycopg (``uv sync --group examples``) and a Postgres: ``TXOUTBOX_PG_DSN``
or the embedded ``pgserver``. Skips otherwise.
"""

import os
import sys
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

django = pytest.importorskip("django")
pytest.importorskip("psycopg")

from asgiref.sync import sync_to_async  # noqa: E402

from txoutbox.testing import StorageContract  # noqa: E402

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


@pytest.fixture(scope="session")
def dsn(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    if env := os.environ.get("TXOUTBOX_PG_DSN"):
        yield env
        return
    try:
        import pgserver
    except ImportError:
        pytest.skip("set TXOUTBOX_PG_DSN or install pgserver to run Postgres tests")
    pgdata = tmp_path_factory.mktemp("pgdata")
    try:
        server = pgserver.get_server(str(pgdata))
    except Exception as exc:
        pytest.skip(f"embedded Postgres unavailable: {exc}")
    try:
        yield server.get_uri()
    finally:
        server.cleanup()


def _django_db(dsn: str) -> dict[str, object]:
    url = urlparse(dsn)
    query = parse_qs(url.query)
    return {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": url.path.lstrip("/"),
        "USER": url.username or "",
        "PASSWORD": url.password or "",
        "HOST": query.get("host", [url.hostname or ""])[0],
        "PORT": url.port or "",
    }


@pytest.fixture(scope="session")
def django_ready(dsn: str) -> Iterator[None]:
    from django.conf import settings
    from django.core.management import call_command

    sys.path.insert(0, str(EXAMPLES))
    settings.configure(
        DATABASES={"default": _django_db(dsn)},
        INSTALLED_APPS=["django_outbox"],
        USE_TZ=True,
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
    )
    django.setup()
    call_command("migrate", run_syncdb=True, verbosity=0)
    yield
    from django.db import connections

    connections.close_all()


class TestDjangoStorage(StorageContract):
    @pytest.fixture
    async def storage(self, django_ready: None) -> AsyncIterator[object]:
        from django_outbox.models import OutboxRow
        from django_outbox.storage import DjangoStorage

        await sync_to_async(OutboxRow.objects.all().delete)()
        yield DjangoStorage()
        await sync_to_async(OutboxRow.objects.all().delete)()

    async def insert(self, storage, topic, payload, key=None):
        from django_outbox.models import OutboxRow

        row = await sync_to_async(OutboxRow.objects.create)(topic=topic, payload=payload, key=key)
        return row.id


class TestDjangoStorageLoose(TestDjangoStorage):
    strict_ordering = False

    @pytest.fixture
    async def storage(self, django_ready: None) -> AsyncIterator[object]:
        from django_outbox.models import OutboxRow
        from django_outbox.storage import DjangoStorage

        await sync_to_async(OutboxRow.objects.all().delete)()
        yield DjangoStorage(strict_ordering=False)
        await sync_to_async(OutboxRow.objects.all().delete)()
