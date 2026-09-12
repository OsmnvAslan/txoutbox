# Django ORM storage for txoutbox

* `models.py`: the outbox table as a model.
* `storage.py`: the five `Storage` methods over the ORM, wrapped with `sync_to_async`, with
  strict per-key ordering (`NOT EXISTS` predicate + advisory lock, like the asyncpg adapter).

Copy both into an app, run `makemigrations`, and start the relay from a management command:

```python
class Command(BaseCommand):
    def handle(self, *args, **options):
        asyncio.run(Relay(DjangoStorage(), publish).run(handle_signals=True))
```

This recipe is not shipped as a package module because it depends on your model. It is verified
against `txoutbox.testing.StorageContract` on PostgreSQL in this repo's test suite
(`tests/test_django_example.py`); run the same contract against your copy.
