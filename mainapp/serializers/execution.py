from rest_framework.serializers import ModelSerializer

from mainapp.models import Execution


class ExecutionSerializer(ModelSerializer):
    class Meta:
        model = Execution
        fields = ("id", "name", "studies")
