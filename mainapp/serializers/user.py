from rest_framework.serializers import ModelSerializer

from mainapp.models import User


class UserSerializer(ModelSerializer):
    class Meta:
        model = User
        fields = ("id", "email", "organization", "name")
