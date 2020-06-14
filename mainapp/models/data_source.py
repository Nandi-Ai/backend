import logging
import uuid

from django.contrib.postgres.fields import JSONField
from django.db import models
from django.db.models import signals
from django.dispatch import receiver

from mainapp.utils import aws_service

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

    class Meta:
        db_table = "data_sources"
        unique_together = (("name", "dataset"),)

    @property
    def glue_table(self):
        if not self.type == "structured":
            return None
        name = self.dir.translate(
            {ord(c): "_" for c in "!@#$%^&*()[]{};:,./<>?\|`~-=_+\ "}
        )
        name = name.lower()
        return name

    @property
    def bucket(self):
        return self.dataset.bucket


@receiver(signals.pre_delete, sender=DataSource)
def delete_data_source(sender, instance, **kwargs):
    data_source = instance
    org_name = instance.dataset.organization.name
    logger.info(
        f"Deleting data source {data_source.name} for following dataset.id {data_source.id}"
    )
    if data_source.glue_table:
        glue_client = aws_service.create_glue_client(org_name=org_name)
        try:
            glue_client.delete_table(
                DatabaseName=data_source.dataset.glue_database,
                Name=data_source.glue_table,
            )
            logger.info(f"Removed glue table: {data_source.glue_table} successfully")
        except glue_client.exceptions.EntityNotFoundException as e:
            logger.warning("Unexpected error when deleting glue table", error=e)

    if data_source.dir:
        if data_source.dir == "":
            logger.warning(
                f"Warning: data source {data_source.name} {data_source.id} 'dir' field is an empty string ('')"
            )
        else:  # delete dir in bucket
            s3_resource = aws_service.create_s3_resource(org_name=org_name)
            try:
                bucket = s3_resource.Bucket(data_source.bucket)
                bucket.objects.filter(Prefix=data_source.dir + "/").delete()
            except s3_resource.exceptions.NoSuchKey:
                logger.warning(
                    f"Warning no such key {data_source.dir} in {data_source.bucket}. "
                    f"Ignoring deleting dir while deleting data_source {data_source.name} ({data_source.id})"
                )
            except s3_resource.exceptions.NoSuchBucket:
                logger.warning(
                    f"Warning no such bucket {data_source.bucket} while trying to delete dir {dir}"
                )

    logger.info(
        f"Data source {data_source.name} ({data_source.id}) was deleted successfully"
    )