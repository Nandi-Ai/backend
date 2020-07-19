import logging
import uuid

from django.db import models
from mainapp.utils import lib

logger = logging.getLogger(__name__)


class Study(models.Model):
    VM_CREATING = "creating"
    VM_STARTING = "starting"
    VM_ACTIVE = "active"
    VM_STOPPING = "stopping"
    VM_STOPPED = "stopped"
    ST_ERROR = "error"
    STUDY_DELETED = "deleted"
    possible_statuses_for_study = (
        (VM_CREATING, "creating"),
        (VM_STARTING, "starting"),
        (VM_ACTIVE, "active"),
        (VM_STOPPING, "stopping"),
        (VM_STOPPED, "stopped"),
        (ST_ERROR, "error"),
        (STUDY_DELETED, "deleted"),
    )
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True, max_length=255)
    datasets = models.ManyToManyField(
        "Dataset", related_name="studies", through="StudyDataset"
    )
    organization = models.ForeignKey(
        "Organization",
        on_delete=models.CASCADE,
        related_name="studies",
        null=False,
        blank=False,
    )
    users = models.ManyToManyField("User", related_name="studies")
    user_created = models.ForeignKey(
        "User", on_delete=models.SET_NULL, related_name="studies_created", null=True
    )
    execution = models.ForeignKey(
        "Execution", on_delete=models.CASCADE, related_name="studies", null=True
    )
    status = models.CharField(
        choices=possible_statuses_for_study,
        max_length=32,
        null=False,
        blank=False,
        default="active",
    )
    tags = models.ManyToManyField("Tag", related_name="study_tags")
    updated_at = models.DateTimeField(auto_now=True)
    cover = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        db_table = "studies"

    @property
    def bucket(self):
        return "lynx-workspace-" + str(self.id)

    def delete_bucket(self, org_name):
        logger.info(f"Deleting bucket {self.bucket} for study {self.name}:{self.id}")
        lib.delete_bucket(bucket_name=self.bucket, org_name=org_name)
        lib.delete_role_and_policy(bucket_name=self.bucket, org_name=org_name)

    def __str__(self):
        return f"<Study id={self.id} name={self.name}>"
