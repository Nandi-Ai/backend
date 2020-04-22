from django.db import models


class StudyDataset(models.Model):
    FULL_ACCESS = "full_access"
    AGGREGATED_ACCESS = "aggregated_access"
    possible_dataset_permission_for_study = (
        (FULL_ACCESS, "full_access"),
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
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "studies_datasets"
        unique_together = ("dataset", "study")

    def __str__(self):
        return f"<StudyDataset dataset={self.dataset} study={self.study} permission={self.permission}>"
