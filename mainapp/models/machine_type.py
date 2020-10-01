import uuid
from django.db import models


class MachineType(models.Model):

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    type = models.CharField(max_length=255, unique=True)
    name = models.CharField(max_length=255)
    vCPUs = models.IntegerField()
    RAM = models.IntegerField()
    GPU = models.CharField(blank=True, max_length=255)
    EC2Instance = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    available_for_batch = models.BooleanField(default=False)

    class Meta:
        db_table = "machine_types"

    def __str__(self):
        return f"<Machine Type id={self.id} type={self.type}>"
