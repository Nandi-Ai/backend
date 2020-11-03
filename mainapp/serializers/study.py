from django.db import transaction
from rest_framework.fields import CurrentUserDefault
from rest_framework.serializers import ModelSerializer

from mainapp.models import Study, StudyDataset, Dataset
from mainapp.serializers.study_dataset import StudyDatasetSerializer
from mainapp.serializers.user import UserSerializer

from mainapp.exceptions.serializers_error import PermissionException, InvalidDataset


class StudySerializer(ModelSerializer):
    # mapping for admin and full access only.
    # if study permission is agg, no matter what permission the user has - he can go in
    # any other permission - only if the user and study permission have the same permission (and keys)
    __STUDY_DATASET_PERMISSION_MAPPING = {
        "admin": ["full_access", "deid_access", "limited_access"],
        "full_access": ["limited_access"],
    }

    datasets = StudyDatasetSerializer(
        source="studydataset_set",
        many=True,
        read_only=False,
        default=CurrentUserDefault(),
    )

    class Meta:
        model = Study
        fields = (
            "id",
            "name",
            "datasets",
            "organization",
            "users",
            "tags",
            "updated_at",
            "created_at",
            "description",
            "user_created",
            "cover",
            "status",
        )
        extra_kwargs = {
            "users": {"allow_empty": True},  # required = False?
            "organization": {"allow_empty": True, "read_only": True},
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
            try:
                dataset_instance = Dataset.objects.get(id=dataset_id)
            except Dataset.DoesNotExist:
                raise InvalidDataset(dataset_id)

            current_user = CurrentUserDefault()(self)
            user_dataset_permission = current_user.permission(dataset_instance)
            user_dataset_permission_attributes = current_user.permission_attributes(
                dataset_instance
            )
            user_requested_permission = dataset.get("permission")
            user_requested_permission_attributes = dataset.get(
                "permission_attributes", dict()
            )

            if not self.__is_authorized(
                user_requested_permission,
                user_requested_permission_attributes,
                user_dataset_permission,
                user_dataset_permission_attributes,
            ):
                raise PermissionException(user_dataset_permission)

            try:
                study_dataset = StudyDataset.objects.get(
                    dataset=dataset_id, study=instance
                )
                prev_datasets.pop(dataset_id)
                study_dataset.permission = user_requested_permission
                study_dataset.permission_attributes = (
                    user_requested_permission_attributes
                )
                study_dataset.save()
            except StudyDataset.DoesNotExist:
                if dataset_instance.organization != instance.organization:
                    raise Exception(
                        "Dataset's organization doesn't match the study organization"
                    )
                StudyDataset.objects.create(
                    study=instance,
                    dataset=dataset_instance,
                    permission=user_requested_permission,
                    permission_attributes=user_requested_permission_attributes,
                )

        if len(prev_datasets) > 0:
            for item in prev_datasets.values():
                study_dataset = StudyDataset.objects.get(
                    dataset=item.id, study=instance
                )
                study_dataset.delete()

        return instance

    def to_representation(self, instance):
        data = super().to_representation(instance)
        users_serializer = UserSerializer(instance.users, many=True, read_only=False)
        data["users"] = users_serializer.data

        return data

    def __is_authorized(
        self,
        user_requested_permission,
        user_requested_permission_attributes,
        user_dataset_permission,
        user_dataset_permission_attributes,
    ):
        if user_requested_permission == "aggregated_access":
            return True
        if user_requested_permission == user_dataset_permission:
            return (
                user_dataset_permission_attributes
                == user_requested_permission_attributes.get("key")
            )
        return user_requested_permission in self.__STUDY_DATASET_PERMISSION_MAPPING.get(
            user_dataset_permission, list()
        )
