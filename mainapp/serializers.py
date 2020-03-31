from django.db import transaction
from rest_framework.serializers import *

from mainapp.models import (
    Organization,
    Execution,
    User,
    Documentation,
    Tag,
    DataSource,
    Activity,
    Study,
    Request,
    Dataset,
    StudyDataset,
)


class ExecutionSerializer(ModelSerializer):
    class Meta:
        model = Execution
        fields = ("id", "name", "studies")


class UserSerializer(ModelSerializer):
    class Meta:
        model = User
        fields = ("id", "email", "organization", "name")


class OrganizationSerializer(ModelSerializer):
    class Meta:
        model = Organization
        fields = ("id", "name", "logo")


class DocumentationSerializer(ModelSerializer):
    def __init__(self, *args, **kwargs):
        many = kwargs.pop("many", True)
        super(DocumentationSerializer, self).__init__(many=many, *args, **kwargs)

    class Meta:
        model = Documentation
        fields = ("id", "dataset", "file_name")


class TagSerializer(ModelSerializer):
    class Meta:
        model = Tag
        fields = ("id", "name", "category")


class DataSourceSerializer(ModelSerializer):
    class Meta:
        model = DataSource
        fields = (
            "id",
            "name",
            "dir",
            "s3_objects",
            "type",
            "about",
            "programmatic_name",
            "dataset",
            "state",
            "glue_table",
            "children",
            "ancestor",
            "cohort",
        )
        extra_kwargs = {
            "state": {"read_only": True},
            "cohort": {"read_only": True},
            "programmatic_name": {"read_only": True},
            "children": {"read_only": True},
        }


class ActivitySerializer(ModelSerializer):
    class Meta:
        model = Activity
        fields = "__all__"
        extra_kwargs = {"user": {"read_only": True}}


class RequestSerializer(ModelSerializer):
    class Meta:
        model = Request
        fields = "__all__"


class DatasetSerializer(ModelSerializer):
    class Meta:
        model = Dataset
        fields = (
            "id",
            "name",
            "admin_users",
            "aggregated_users",
            "full_access_users",
            "is_discoverable",
            "default_user_permission",
            "user_created",
            "updated_at",
            "created_at",
            "tags",
            "readme",
            "description",
            "organization",
            "state",
            "programmatic_name",
            "bucket",
            "cover",
            "children",
            "ancestor",
        )

        extra_kwargs = {
            "children": {"read_only": True},
            "tags": {"allow_empty": True},
            "admin_users": {"allow_empty": True},
            "aggregated_users": {"allow_empty": True},
            "full_access_users": {"allow_empty": True},
            "user_created": {"read_only": True},
            "bucket": {"read_only": True},
            "programmatic_name": {"read_only": True},
            "updated_at": {"read_only": True},
            "created_at": {"read_only": True},
        }


class StudyDatasetSerializer(ModelSerializer):
    class Meta:
        model = StudyDataset
        fields = ("dataset", "permission", "updated_at", "created_at")
        extra_kwargs = {
            "updated_at": {"read_only": True},
            "created_at": {"read_only": True},
        }


class StudySerializer(ModelSerializer):
    # users = ListField(required=False, write_only=True)
    datasets = StudyDatasetSerializer(
        source="studydataset_set", many=True, read_only=False
    )

    class Meta:
        model = Study
        fields = (
            "id",
            "name",
            "datasets",
            "users",
            "tags",
            "updated_at",
            "description",
            "user_created",
            "cover",
        )
        extra_kwargs = {
            "users": {"allow_empty": True},  # required = False?
            "datasets": {"allow_empty": True},
            "tags": {"allow_empty": True},
        }

    @transaction.atomic
    def update(self, instance, validated_data):
        instance.__dict__.update(**validated_data)
        instance.tags.set(validated_data.get("tags", instance.tags))
        instance.users.set(validated_data.get("users", instance.users))
        instance.save()

        prev_datasets = {
            str(dataset.id): dataset for dataset in instance.datasets.all()
        }
        for dataset in self.initial_data.get("datasets"):
            dataset_id = dataset.get("dataset")
            permission = dataset.get("permission")
            try:
                study_dataset = StudyDataset.objects.get(
                    dataset=dataset_id, study=instance
                )
                prev_datasets.pop(dataset_id)
                study_dataset.permission = permission
                study_dataset.save()
            except StudyDataset.DoesNotExist:
                try:
                    dataset_instance = Dataset.objects.get(id=dataset_id)
                except Dataset.DoesNotExist:
                    raise Exception(f"Dataset instance {dataset_id} not exist")
                StudyDataset.objects.create(
                    study=instance, dataset=dataset_instance, permission=permission
                )

        if len(prev_datasets) > 0:
            for item in prev_datasets.values():
                item.delete()

        return instance


class SimpleQuerySerializer(Serializer):
    query_string = CharField(max_length=2048)
    dataset_id = CharField(max_length=255)


class QuerySerializer(Serializer):
    query = CharField(max_length=2048, required=False, default=None)
    filter = CharField(max_length=2048, required=False)
    columns = CharField(max_length=2048, required=False)
    limit = IntegerField(required=False, default=None)
    sample_aprx = IntegerField(required=False, default=None)
    dataset_id = CharField(max_length=255)
    data_source_id = CharField(max_length=255)


class CohortSerializer(Serializer):
    filter = CharField(max_length=2048, required=False)
    columns = CharField(max_length=2048, required=False)
    limit = IntegerField(required=False, default=None)
    dataset_id = CharField(max_length=255)
    data_source_id = CharField(max_length=255)
    destination_dataset_id = CharField(max_length=255)


class DatasetUploadedSerializer(Serializer):
    query = IntegerField()
    catalogs = ListField()
