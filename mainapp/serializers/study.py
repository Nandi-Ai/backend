from django.db import transaction
from rest_framework.fields import CurrentUserDefault
from rest_framework.serializers import ModelSerializer

from mainapp.models import Study, StudyDataset, Dataset
from mainapp.serializers.study_dataset import StudyDatasetSerializer
from mainapp.serializers.user import UserSerializer


class StudySerializer(ModelSerializer):
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
            permission = dataset.get("permission")
            permission_attributes = dataset.get("permission_attributes")
            try:
                study_dataset = StudyDataset.objects.get(
                    dataset=dataset_id, study=instance
                )
                prev_datasets.pop(dataset_id)
                study_dataset.permission = permission
                study_dataset.permission_attributes = permission_attributes
                study_dataset.save()
            except StudyDataset.DoesNotExist:
                try:
                    dataset_instance = Dataset.objects.get(id=dataset_id)
                except Dataset.DoesNotExist:
                    raise Exception(f"Dataset instance {dataset_id} not exist")
                if dataset_instance.organization != instance.organization:
                    raise Exception(
                        "Dataset's organization doesn't match the study organization"
                    )
                if permission == "deid_access":
                    user = CurrentUserDefault()(self)
                    current_user_id = user.id
                    if permission_attributes and permission_attributes.key:
                        # verify user is admin / full when explicitly selecting method
                        if user.permission(dataset_instance) not in [
                            "admin",
                            "full_access",
                        ]:
                            raise Exception(
                                "No permission for user to add this dataset permission"
                            )
                    else:
                        user_dataset_permission = dataset_instance.datasetuser_set.filter(
                            user=current_user_id
                        )
                        if not user_dataset_permission:
                            raise Exception(
                                f"User have no permission for dataset {dataset_instance.id}"
                            )

                        permission_attributes = (
                            user_dataset_permission.first().permission_attributes
                        )
                        if (
                            not permission_attributes
                            or "key" not in permission_attributes
                        ):
                            raise Exception(
                                f"Missing permission attributes for user in dataset {dataset_instance.id}"
                            )

                StudyDataset.objects.create(
                    study=instance,
                    dataset=dataset_instance,
                    permission=permission,
                    permission_attributes=permission_attributes,
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
