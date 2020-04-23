from rest_framework.serializers import ModelSerializer

from mainapp.models import Dataset


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
