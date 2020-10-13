from django.contrib.postgres.fields import JSONField
from django.db import models
import logging

logger = logging.getLogger(__name__)


class DataSourceMethod(models.Model):
    READY = "ready"
    PENDING = "pending"
    ERROR = "error"

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

    def __set_state(self, state):
        if self.state == state:
            logger.warning(
                f"Human! Somewhere in your code you're trying to set the data-source-method {self.id} state "
                f"to {state} when it's already in {state} state."
            )
        else:
            self.state = state
            self.save()

    def set_as_pending(self):
        self.__set_state(DataSourceMethod.PENDING)

    def set_as_ready(self):
        self.__set_state(DataSourceMethod.READY)

    def set_as_error(self):
        self.__set_state(DataSourceMethod.ERROR)

    def is_ready(self):
        return self.state == DataSourceMethod.READY
