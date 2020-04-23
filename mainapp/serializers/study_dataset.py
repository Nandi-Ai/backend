from rest_framework.serializers import ModelSerializer

from mainapp.models import StudyDataset


class StudyDatasetSerializer(ModelSerializer):
    class Meta:
        model = StudyDataset
        fields = ("dataset", "permission", "updated_at", "created_at")
        extra_kwargs = {
            "updated_at": {"read_only": True},
            "created_at": {"read_only": True},
        }
