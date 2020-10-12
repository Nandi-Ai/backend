import logging
import uuid

from django.contrib.postgres.fields import JSONField
from django.db import models

from mainapp.settings import DELETE_DATASETS_FROM_DATABASE
from mainapp.utils import lib, aws_service
from mainapp.utils.dataset import delete_aws_resources_for_dataset
from .dataset_user import DatasetUser
from .study import Study

logger = logging.getLogger(__name__)


class Dataset(models.Model):
    states = (("public", "public"), ("private", "private"), ("archived", "archived"))

    possible_default_user_permissions_for_private_dataset = (
        ("none", "none"),
        ("aggregated_access", "aggregated_access"),
        ("limited_access", "limited_access"),
        ("deid_access", "deid_access"),
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
    users = models.ManyToManyField(
        "User", related_name="permitted_datasets", through="DatasetUser"
    )
    user_created = models.ForeignKey(
        "User", on_delete=models.SET_NULL, related_name="datasets_created", null=True
    )
    starred_users = models.ManyToManyField("User", related_name="starred_datasets")
    tags = models.ManyToManyField("Tag", related_name="dataset_tags")
    state = models.CharField(choices=states, max_length=32)
    is_discoverable = models.BooleanField(blank=False, null=False)
    default_user_permission = models.CharField(
        choices=possible_default_user_permissions_for_private_dataset,
        max_length=32,
        null=True,
    )
    permission_attributes = JSONField(null=True, default=None, blank=True)
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
    is_deleted = models.BooleanField(default=False)

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

    def delete(self, using=None, keep_parents=False):
        # Deleting AWS resources MUST be above the deletion part, as if
        # DELETE_DATASETS_FROM_DATABASE is set to true,
        # the dataset object in the database will be deleted
        # and there will be no more way to find the resources by `id`, `glue_database`, etc...
        delete_aws_resources_for_dataset(dataset=self, org_name=self.organization.name)
        # we need to set is_deleted even if DELETE_DATASETS_FROM_DATABASE as
        # the dataset_user post delete receiver verify if deleted to ignore adding Activity events
        self.is_deleted = True
        self.save()
        if DELETE_DATASETS_FROM_DATABASE:
            # This will trigger data_source `delete_data_source` @receiver also as there is a CASCADE set onDelete.
            super(Dataset, self).delete()

    def query(self, query):
        client = aws_service.create_athena_client(org_name=self.organization.name)

        return client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": self.glue_database},
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

    @property
    def permission_key(self):
        return self.permission_attributes.get("key")

    def __filtered_dataset_users(self, permission):
        return self.datasetuser_set.filter(permission=permission)

    @property
    def limited_dataset_users(self):
        return self.__filtered_dataset_users(DatasetUser.LIMITED_ACCESS)

    def has_pending_datasource(self):
        return any(data_source.is_pending() for data_source in self.data_sources.all())

    @property
    def study_datasets(self):
        for studydataset in self.studydataset_set.all():
            if studydataset.study.status is not Study.STUDY_DELETED:
                yield studydataset
        return

    def __str__(self):
        return f"<Dataset id={self.id} name={self.name}>"
