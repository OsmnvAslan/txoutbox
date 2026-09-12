"""The outbox table as a Django model. Put it in any app; ``makemigrations`` does the rest."""

from django.db import models


class OutboxRow(models.Model):
    topic = models.CharField(max_length=200)
    key = models.CharField(max_length=200, null=True, blank=True)
    payload = models.BinaryField()
    headers = models.JSONField(default=dict)
    status = models.CharField(max_length=8, default="pending")
    attempts = models.PositiveIntegerField(default=0)
    retry_at = models.DateTimeField(null=True, blank=True)
    leased_by = models.CharField(max_length=100, null=True, blank=True)
    lease_until = models.DateTimeField(null=True, blank=True)
    last_error = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [  # noqa: RUF012 - Django Meta convention
            models.Index(fields=["status", "id"], name="outbox_pending_idx"),
            models.Index(fields=["key", "id"], name="outbox_key_idx"),
        ]
