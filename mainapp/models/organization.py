import uuid

from django.db import models


class Organization(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    logo = models.CharField(max_length=255, null=True)
    default = models.BooleanField(default=False)

    def set_default(self):
        Organization.objects.all().update(default=False)
        self.default = True
        self.save()

    class Meta:
        db_table = "organizations"
