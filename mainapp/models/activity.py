import uuid

from django.contrib.postgres.fields import JSONField
from django.db import models


class Activity(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    ts = models.DateTimeField(auto_now_add=True)
    user = models.ForeignKey(
        "User", on_delete=models.SET_NULL, related_name="activities", null=True
    )
    dataset = models.ForeignKey(
        "Dataset", on_delete=models.SET_NULL, related_name="activities", null=True
    )
    study = models.ForeignKey(
        "Study", on_delete=models.SET_NULL, related_name="activities", null=True
    )
    type = models.CharField(null=True, blank=True, max_length=32)
    note = models.CharField(null=True, blank=True, max_length=2048)
    meta = JSONField(null=True, blank=True, default=None)

    class Meta:
        db_table = "activities"
