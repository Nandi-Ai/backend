import uuid

from django.db import models
from django.db.models import signals
from django.dispatch import receiver

from mainapp.utils.deidentification.images_de_id import ImageDeId
from .data_source_method import DataSourceMethod
from .study_dataset import StudyDataset


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

        if data_source_methods_states[self.ERROR]:
            return self.ERROR

        if data_source_methods_states[self.PENDING]:
            return self.PENDING

        return self.READY


@receiver(signals.pre_delete, sender=Method)
def remove_dataset_from_studies(sender, instance, using, **kwargs):
    """
    When method is being deleted, remove the dataset from all studies that have de-id permission with this method
    """
    for study_dataset in StudyDataset.objects.filter(
        permission=StudyDataset.DE_IDENTIFIED, dataset=instance.dataset
    ):
        # without str it does NOT working
        if str(study_dataset.permission_key) == str(instance.id):
            study_dataset.delete()


@receiver(signals.post_delete, sender=Method)
def remove_files(sender, instance, using, **kwargs):
    """
    Delete all de-id files related to this method
    """
    for dsrc_method in instance.data_source_methods.all():
        data_source = dsrc_method.data_source
        image_de_id = ImageDeId(
            org_name=data_source.dataset.organization.name,
            data_source=data_source,
            dsrc_method=dsrc_method,
        )
        image_de_id.delete()
