from rest_framework.serializers import ModelSerializer

from mainapp.models import User


class UserSerializer(ModelSerializer):
    class Meta:
        model = User
        fields = (
            "id",
            "email",
            "organization",
            "name",
            "display_name",
            "title",
            "phone_number",
            "department",
            "linkedin",
            "bio",
            "tags",
            "photo",
            "interests",
            "latest_eula_file_path",
            "is_signed_eula",
        )
        extra_kwargs = {
            "id": {"read_only": True},
            "email": {"read_only": True},
            "organization": {"read_only": True},
            "latest_eula_file_path": {"read_only": True},
            "is_signed_eula": {"read_only": True},
        }
