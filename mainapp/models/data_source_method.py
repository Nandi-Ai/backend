from django.contrib.postgres.fields import JSONField
from django.db import models


class DataSourceMethod(models.Model):
    method = models.ForeignKey(
        "Method",
        on_delete=models.CASCADE,
        related_name="data_source_methods",
        null=False,
        blank=False,
    )
    data_source = models.ForeignKey(
        "DataSource",
        on_delete=models.CASCADE,
        related_name="methods",
        null=False,
        blank=False,
    )
    included = models.BooleanField(default=True)
    attributes = JSONField(default=dict)
    state = models.CharField(default="pending", blank=True, max_length=32)

    class Meta:
        db_table = "data_source_methods"
        unique_together = ("method", "data_source")
