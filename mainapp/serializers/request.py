from rest_framework.serializers import ModelSerializer

from mainapp.models import Request
from mainapp.serializers.user import UserSerializer


class RequestSerializer(ModelSerializer):
    class Meta:
        model = Request
        fields = "__all__"

    def to_representation(self, instance):
        data = super().to_representation(instance)
        user_requested_serializer = UserSerializer(
            instance.user_requested, many=False, read_only=False
        )
        data["user_requested"] = user_requested_serializer.data

        return data
