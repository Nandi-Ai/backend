from rest_framework.serializers import ModelSerializer

from mainapp.models import Activity


class ActivitySerializer(ModelSerializer):
    class Meta:
        model = Activity
        fields = "__all__"
        extra_kwargs = {"user": {"read_only": True}}
