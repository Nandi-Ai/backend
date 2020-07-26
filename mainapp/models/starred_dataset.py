import uuid
from django.db import models


class StarredDataset(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey("User", on_delete=models.CASCADE, null=False)
    dataset = models.ForeignKey("Dataset", on_delete=models.CASCADE, null=False)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "starred_datasets"
        unique_together = ("user", "dataset")
