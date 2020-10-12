from django.contrib.postgres.fields import JSONField
from django.db import models
from django.db.models import signals
from django.dispatch import receiver

from mainapp.utils.lib import create_limited_table_for_dataset


class StudyDataset(models.Model):
    FULL_ACCESS = "full_access"
    LIMITED_ACCESS = "limited_access"
    DEIDENTIFIED = "deid_access"
    SYNTHETIC = "synthetic_access"
    AGGREGATED_ACCESS = "aggregated_access"
    possible_dataset_permission_for_study = (
        (FULL_ACCESS, "full_access"),
        (LIMITED_ACCESS, "limited_access"),
        (DEIDENTIFIED, "deid_access"),
        (SYNTHETIC, "synthetic_access"),
        (AGGREGATED_ACCESS, "aggregated_access"),
    )
    dataset = models.ForeignKey("Dataset", on_delete=models.CASCADE)
    study = models.ForeignKey("Study", on_delete=models.CASCADE)
    permission = models.CharField(
        choices=possible_dataset_permission_for_study,
        max_length=32,
        null=False,
        blank=False,
    )
    permission_attributes = JSONField(null=True, default=None)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "studies_datasets"
        unique_together = ("dataset", "study")

    def __str__(self):
        return f"<StudyDataset dataset={self.dataset} study={self.study} permission={self.permission}>"

    @property
    def permission_key(self):
        return self.permission_attributes.get("key")

    def process(self):
        """
        trigger process to the dataset attached to the study (if needed)
        """
        if self.permission == StudyDataset.LIMITED_ACCESS:
            create_limited_table_for_dataset(self.dataset, self.permission_key)


@receiver(signals.post_save, sender=StudyDataset)
def study_dataset_post_save(sender, instance, **kwargs):
    instance.process()
