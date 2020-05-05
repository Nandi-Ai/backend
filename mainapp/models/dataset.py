import logging
import uuid

from django.db import models
from django.db.models import signals
from django.dispatch import receiver

from mainapp.exceptions import BucketNotFound, PolicyNotFound, RoleNotFound
from mainapp.utils import lib, aws_service

logger = logging.getLogger(__name__)


class Dataset(models.Model):
    states = (("public", "public"), ("private", "private"), ("archived", "archived"))

    possible_default_user_permissions_for_private_dataset = (
        ("none", "none"),
        ("aggregated_access", "aggregated_access"),
    )
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True, max_length=255)
    readme = models.TextField(blank=True, null=True)
    admin_users = models.ManyToManyField("User", related_name="admin_datasets")
    aggregated_users = models.ManyToManyField(
        "User", related_name="aggregated_datasets"
    )
    full_access_users = models.ManyToManyField(
        "User", related_name="full_access_datasets"
    )
    user_created = models.ForeignKey(
        "User", on_delete=models.SET_NULL, related_name="datasets_created", null=True
    )
    tags = models.ManyToManyField("Tag", related_name="dataset_tags")
    state = models.CharField(choices=states, max_length=32)
    is_discoverable = models.BooleanField(blank=False, null=False)
    default_user_permission = models.CharField(
        choices=possible_default_user_permissions_for_private_dataset,
        max_length=32,
        null=True,
    )
    bucket_override = models.CharField(max_length=255, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)
    glue_database_override = models.CharField(max_length=255, blank=True, null=True)
    programmatic_name = models.CharField(max_length=255, blank=True, null=True)
    organization = models.ForeignKey(
        "Organization", on_delete=models.DO_NOTHING, related_name="datasets", null=True
    )
    cover = models.CharField(max_length=255, blank=True, null=True)
    ancestor = models.ForeignKey(
        "self", on_delete=models.SET_NULL, related_name="children", null=True
    )

    class Meta:
        db_table = "datasets"

    @property
    def permitted_users(self):
        return (
            self.aggregated_users | self.admin_users | self.full_access_users
        ).distinct()

    @property
    def glue_database(self):
        if self.glue_database_override:
            return self.glue_database_override
        return "dataset-" + str(self.id)

    @property
    def bucket(self):
        if self.bucket_override:
            return self.bucket_override
        return "lynx-dataset-" + str(self.id)

    def delete_bucket(self, org_name):
        logger.info(f"Deleting bucket {self.bucket} for dataset {self.id}")
        lib.delete_bucket(bucket_name=self.bucket, org_name=org_name)
        lib.delete_role_and_policy(bucket_name=self.bucket, org_name=org_name)

    def query(self, query):
        client = aws_service.create_athena_client(org_name=self.organization.name)

        return client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                "Database": self.glue_database  # the name of the database in glue/athena
            },
            ResultConfiguration={
                "OutputLocation": f"s3://{self.bucket}/temp_execution_results"
            },
        )

    def get_s3_object(self, key):
        return lib.get_s3_object(
            bucket=self.bucket, key=key, org_name=self.organization.name
        )

    def get_query_execution(self, query_execution_id):
        return self.get_s3_object(
            key="temp_execution_results/" + query_execution_id + ".csv"
        )

    def get_columns_types(self, glue_table):
        return lib.get_columns_types(
            org_name=self.organization.name,
            glue_database=self.glue_database,
            glue_table=glue_table,
        )

    def __str__(self):
        return f"<Dataset id={self.id} name={self.name}>"


@receiver(signals.pre_delete, sender=Dataset)
def delete_dataset(sender, instance, **kwargs):
    dataset = instance
    try:
        dataset.delete_bucket(org_name=dataset.organization.name)
    except BucketNotFound as e:
        logger.warning(
            f"Bucket {e.bucket_name} was not found for dataset id {dataset.id} at delete bucket operation"
        )
    except PolicyNotFound as e:
        logger.warning(
            f"Policy {e.policy} was not found for dataset id {dataset.id} at delete bucket operation"
        )
    except RoleNotFound as e:
        logger.warning(
            f"Role {e.role} was not found for dataset id {dataset.id} at delete bucket operation"
        )
