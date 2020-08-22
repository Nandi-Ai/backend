import uuid

from django.contrib.postgres.fields import JSONField
from django.db import models


class Request(models.Model):
    types = (("dataset_access", "dataset_access"),)

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    user_requested = models.ForeignKey(
        "User", on_delete=models.CASCADE, related_name="requests", null=True
    )
    dataset = models.ForeignKey(
        "Dataset", on_delete=models.CASCADE, related_name="requests", null=True
    )
    study = models.ForeignKey(
        "Study", on_delete=models.CASCADE, related_name="requests", null=True
    )
    type = models.CharField(choices=types, max_length=32)
    note = models.CharField(null=True, blank=True, max_length=2048)
    permission = models.CharField(null=True, blank=True, max_length=32)
    permission_attributes = JSONField(null=True, default=None)
    state = models.CharField(null=True, blank=True, default="pending", max_length=32)

    class Meta:
        db_table = "requests"
