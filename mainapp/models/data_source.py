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
from mainapp.utils.elasticsearch_service import MonitorEvents, ElasticsearchService

logger = logging.getLogger(__name__)


class DataSource(models.Model):
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

    ElasticsearchService.write_monitoring_event(
        event_type=MonitorEvents.EVENT_DATASET_REMOVE_DATASOURCE,
        datasource_id=data_source.id,
        datasource_name=data_source.name,
        dataset_id=data_source.dataset.id,
        dataset_name=data_source.dataset.name,
        environment_name=data_source.dataset.organization.name,
    )
    logger.info(
        f"Datasource Event: {MonitorEvents.EVENT_DATASET_REMOVE_DATASOURCE.value} "
        f"on dataset {data_source.dataset.name}:{data_source.dataset.id} "
        f"and datasource {data_source.name}:{data_source.id} "
        f"in org {data_source.dataset.organization}"
    )
