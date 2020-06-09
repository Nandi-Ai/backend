from rest_framework.serializers import ModelSerializer

from mainapp.models import Activity, User
from mainapp.serializers.user import UserSerializer


class ActivitySerializer(ModelSerializer):
    class Meta:
        model = Activity
        fields = "__all__"
        extra_kwargs = {"user": {"read_only": True}}

    def to_representation(self, instance):
        data = super().to_representation(instance)

        user_serializer = UserSerializer(instance.user)
        data["user"] = user_serializer.data

        if (
            hasattr(instance, "meta")
            and type(instance.meta) is dict
            and instance.meta.get("user_affected", None)
        ):
            meta_user_serializer = UserSerializer(
                User.objects.filter(id=instance.meta["user_affected"])[0]
            )
            data["meta"]["user_affected"] = meta_user_serializer.data

        return data
