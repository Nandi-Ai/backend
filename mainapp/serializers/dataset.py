from rest_framework.serializers import ModelSerializer
from mainapp.models import Dataset
from mainapp.serializers.user import UserSerializer


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
            "studies",
            "starred_users",
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
            "studies": {"read_only": True},
            "starred_users": {"allow_empty": True, "read_only": True},
        }

        def get_request_user(self):
            request = self.context.get("request")
            if not (request and hasattr(request, "user")):
                return None
            return request.user

        def get_admin_users(self, obj):
            user = self.get_request_user()
            if user:
                return user.id in obj.admin_users

        def get_full_access_users(self, obj):
            user = self.get_request_user()
            if user:
                return user.id in obj.full_access_users

        def get_aggregated_users(self, obj):
            user = self.get_request_user()
            if user:
                return user.id in obj.aggregated_users

    def to_representation(self, instance):
        data = super().to_representation(instance)
        admin_users_serializer = UserSerializer(
            instance.admin_users, many=True, read_only=False
        )
        data["admin_users"] = admin_users_serializer.data

        full_access_serializer = UserSerializer(
            instance.full_access_users, many=True, read_only=False
        )
        data["full_access_users"] = full_access_serializer.data

        aggregated_users_serializer = UserSerializer(
            instance.aggregated_users, many=True, read_only=False
        )
        data["aggregated_users"] = aggregated_users_serializer.data

        return data
