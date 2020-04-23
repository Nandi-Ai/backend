import uuid

from django.db import models


class Tag(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    category = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        db_table = "tags"
        unique_together = (("name", "category"),)

    def __str__(self):
        return f"<Category name={self.name} category={self.category}>"
