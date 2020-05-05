from django.db import transaction
from mainapp.serializers.study_dataset import StudyDatasetSerializer
from rest_framework.serializers import ModelSerializer

from mainapp.models import Study, StudyDataset, Dataset


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
                study_dataset = StudyDataset.objects.get(
                    dataset=item.id, study=instance
                )
                study_dataset.delete()

        return instance
