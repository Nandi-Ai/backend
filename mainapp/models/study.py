import logging
import uuid

from django.db import models
from django.db.models import signals
from django.dispatch import receiver

from mainapp.exceptions import BucketNotFound, RoleNotFound, PolicyNotFound
from mainapp.utils import lib

logger = logging.getLogger(__name__)


class Study(models.Model):
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


@receiver(signals.pre_delete, sender=Study)
def delete_study(sender, instance, **kwargs):
    study = instance
    org_name = study.organization.name

    try:
        study.delete_bucket(org_name=org_name)
    except BucketNotFound as e:
        logger.warning(
            f"Bucket {e.bucket_name} was not found for study {study.name}:{study.id} at delete bucket operation"
        )
    except PolicyNotFound as e:
        logger.warning(
            f"Policy {e.policy} was not found for study {study.name}:{study.id} at delete bucket operation"
        )
    except RoleNotFound as e:
        logger.warning(
            f"Role {e.role} was not found for study {study.name}:{study.id} at delete bucket operation"
        )
