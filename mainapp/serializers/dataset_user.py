from rest_framework.serializers import ModelSerializer

from mainapp.models import DatasetUser
from .user import UserSerializer


class DatasetUserSerializer(ModelSerializer):
    class Meta:
        model = DatasetUser
        fields = (
            "user",
            "permission",
            "permission_attributes",
            "updated_at",
            "created_at",
        )
        extra_kwargs = {
            "updated_at": {"read_only": True},
            "created_at": {"read_only": True},
        }

    def to_representation(self, instance):
        data = super().to_representation(instance)
        user_serializer = UserSerializer(instance.user, many=False, read_only=False)
        data["user"] = user_serializer.data

        return data
