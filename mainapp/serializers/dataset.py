from django.db import transaction
from rest_framework.serializers import ModelSerializer, ValidationError

# noinspection PyPackageRequirements
from slugify import slugify

from mainapp.models import Dataset, DatasetUser, Tag, User
from mainapp.serializers.user import UserSerializer
from mainapp.serializers.dataset_user import DatasetUserSerializer


class DatasetSerializer(ModelSerializer):
    users = DatasetUserSerializer(source="datasetuser_set", many=True, read_only=False)

    class Meta:
        model = Dataset
        fields = (
            "id",
            "name",
            "admin_users",
            "aggregated_users",
            "full_access_users",
            "users",
            "is_discoverable",
            "default_user_permission",
            "permission_attributes",
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
            "users": {"allow_empty": True},
            "user_created": {"read_only": True},
            "bucket": {"read_only": True},
            "programmatic_name": {"read_only": True},
            "updated_at": {"read_only": True},
            "created_at": {"read_only": True},
            "studies": {"read_only": True},
            "starred_users": {"allow_empty": True, "read_only": True},
        }

        def validate(self, data):
            for user in data["aggregated_data"]:
                if user.id in data["full_access"]:
                    raise ValidationError("user already exist")
            return data

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

    def __get_permission_users(self, user_list):
        return list(User.objects.filter(id__in=[user.id for user in user_list]))

    @transaction.atomic
    def create(self, validated_data):
        dataset_admin_users = set(validated_data.pop("admin_users"))
        dataset_agg_users = set(validated_data.pop("aggregated_users"))
        dataset_full_users = set(validated_data.pop("full_access_users"))
        dataset_users = set(validated_data.pop("datasetuser_set"))
        dataset_tags = set(validated_data.pop("tags"))

        request = self.context.get("request")
        validated_data["user_created"] = request.user
        validated_data["organization"] = getattr(
            validated_data.get("ancestor"), "organization", request.user.organization
        )
        validated_data["is_discoverable"] = (
            validated_data["is_discoverable"]
            if validated_data["state"] == "private"
            else True
        )

        if validated_data.get("ancestor"):
            validated_data["is_discoverable"] = False
            validated_data["state"] = "private"

            if validated_data.get("default_user_permission") is None:
                validated_data["default_user_permission"] = "None"
                dataset_full_users.add(request.user)

            elif validated_data["default_user_permission"] == "aggregated_access":
                dataset_agg_users.add(request.user)

        dataset = Dataset.objects.create(**validated_data)
        dataset.programmatic_name = (
            f"{slugify(dataset.name)}-{str(dataset.id).split('-')[0]}"
        )
        dataset.admin_users.set(self.__get_permission_users(dataset_admin_users))
        dataset.full_access_users.set(self.__get_permission_users(dataset_full_users))
        dataset.aggregated_users.set(self.__get_permission_users(dataset_agg_users))
        dataset.tags.set(Tag.objects.filter(id__in=[tag.id for tag in dataset_tags]))

        for dataset_user in dataset_users:
            DatasetUser.objects.create(dataset=dataset, **dataset_user)

        return dataset

    @transaction.atomic
    def update(self, instance, validated_data):
        instance.__dict__.update(**validated_data)
        instance.admin_users.set(
            validated_data.get("admin_users", instance.admin_users)
        )
        instance.aggregated_users.set(
            validated_data.get("aggregated_users", instance.aggregated_users)
        )
        instance.full_access_users.set(
            validated_data.get("full_access_users", instance.full_access_users)
        )
        instance.tags.set(validated_data.get("tags", instance.tags))
        instance.starred_users.set(
            validated_data.get("starred_users", instance.starred_users.all())
        )
        instance.save()

        prev_users = {str(user.id): user for user in instance.users.all()}
        for user in self.initial_data.get("users"):
            user_id = user.get("user")
            permission = user.get("permission")
            permission_attributes = user.get("permission_attributes")
            try:
                dataset_user = DatasetUser.objects.get(user=user_id, dataset=instance)
                prev_users.pop(user_id)
                dataset_user.permission = permission
                dataset_user.permission_attributes = permission_attributes
                dataset_user.save()
                dataset_user.process()
            except DatasetUser.DoesNotExist:
                try:
                    user_instance = User.objects.get(id=user_id)
                except User.DoesNotExist:
                    raise Exception(f"User instance {user_id} not exist")

                dataset_user = DatasetUser.objects.create(
                    dataset=instance,
                    user=user_instance,
                    permission=permission,
                    permission_attributes=permission_attributes,
                )
                dataset_user.process()

        if len(prev_users) > 0:
            for item in prev_users.values():
                dataset_user = DatasetUser.objects.get(user=item.id, dataset=instance)
                dataset_user.delete()

        return instance

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

        user_created_serializer = UserSerializer(
            instance.user_created, many=False, read_only=False
        )
        data["user_created"] = user_created_serializer.data

        return data
