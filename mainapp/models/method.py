import uuid

from django.db import models
from mainapp.models import DataSourceMethod


class Method(models.Model):
    READY = "ready"
    PENDING = "pending"
    ERROR = "error"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    dataset = models.ForeignKey(
        "Dataset",
        on_delete=models.CASCADE,
        related_name="methods",
        null=False,
        blank=False,
    )
    salt_key = models.UUIDField(
        unique=True, null=False, blank=False, default=uuid.uuid4
    )
    group_age_over = models.BooleanField(default=False)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "methods"
        unique_together = ("dataset", "name")

    @property
    def state(self):
        data_source_methods = DataSourceMethod.objects.filter(method_id=self.id)
        data_source_methods_states = {self.ERROR: 0, self.PENDING: 0, self.READY: 0}

        for dsrc_method in data_source_methods:
            data_source_methods_states[dsrc_method.state] += 1

        # If there's at least one pending data source method, the method is pending
        if data_source_methods_states[self.PENDING]:
            return self.PENDING

        # If there aren't any ready or pending data source methods, all data source methods are errored, which means
        # the method is errored
        if not data_source_methods_states[self.READY]:
            return self.ERROR

        # Otherwise the method is ready
        return self.READY
