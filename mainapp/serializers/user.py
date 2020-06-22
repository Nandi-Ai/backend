from rest_framework.serializers import ModelSerializer

from mainapp.models import User


class UserSerializer(ModelSerializer):
    class Meta:
        model = User
        fields = ("id", "email", "organization", "name")
        extra_kwargs = {
            "id": {"read_only": True},
            "email": {"read_only": True},
            "organization": {"read_only": True},
        }
