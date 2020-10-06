import logging
import uuid

from django.contrib.postgres.fields import JSONField
from django.db import models
from django.db.models import signals
from django.dispatch import receiver

from mainapp.exceptions.limited_key_invalid_exception import LimitedKeyInvalidException
from mainapp.utils.data_source import (
    delete_data_source_glue_tables,
    delete_data_source_files_from_bucket,
)
from mainapp.utils.monitoring import handle_event, MonitorEvents

logger = logging.getLogger(__name__)


class DataSource(models.Model):
    READY = "ready"
    PENDING = "pending"
    ERROR = "error"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    dir = models.CharField(null=True, blank=True, max_length=255)
    s3_objects = JSONField(null=True, blank=True, default=None)
    dataset = models.ForeignKey(
        "Dataset", on_delete=models.CASCADE, related_name="data_sources"
    )
    type = models.CharField(null=True, blank=True, max_length=32)
    about = models.TextField(null=True, blank=True, max_length=2048)
    state = models.CharField(null=True, blank=True, max_length=32)
    programmatic_name = models.CharField(max_length=255, blank=True, null=True)
    ancestor = models.ForeignKey(
        "self", on_delete=models.SET_NULL, related_name="children", null=True
    )
    cohort = JSONField(null=True, blank=True, default=None)
    glue_table = models.CharField(null=True, blank=True, max_length=255)

    class Meta:
        db_table = "data_sources"
        unique_together = (("name", "dataset"),)

    @property
    def bucket(self):
        return self.dataset.bucket

    @property
    def limited_value(self):
        dataset = self.dataset
        if dataset.default_user_permission == "limited_access":
            limited = dataset.permission_attributes.get("key")
            try:
                return int(limited)
            except ValueError:
                raise LimitedKeyInvalidException(self)

    def get_limited_glue_table_name(self, limited):
        return f"{self.dir}_limited_{limited}"

    def __set_state(self, state):
        if self.state == state:
            logger.warning(
                f"Human! Somewhere in your code you're trying to set the data-source {self.id} state "
                f"to {state} when it's already in {state} state."
            )
        else:
            self.state = state
            self.save()

    def set_as_pending(self):
        self.__set_state(DataSource.PENDING)

    def set_as_ready(self):
        self.__set_state(DataSource.READY)

    def set_as_error(self):
        self.__set_state(DataSource.ERROR)

    def is_ready(self):
        return self.state != DataSource.READY


@receiver(signals.pre_delete, sender=DataSource)
def delete_data_source(sender, instance, **kwargs):
    data_source = instance
    org_name = instance.dataset.organization.name
    logger.info(
        f"Deleting data source {data_source.name}:{data_source.id}"
        f"for following dataset {data_source.dataset.name}:{data_source.dataset.id}"
        f"in org {data_source.dataset.organization.name}"
    )
    delete_data_source_glue_tables(data_source=data_source, org_name=org_name)
    delete_data_source_files_from_bucket(data_source=data_source, org_name=org_name)

    handle_event(
        MonitorEvents.EVENT_DATASET_REMOVE_DATASOURCE, {"datasource": data_source}
    )
