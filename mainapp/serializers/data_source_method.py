from rest_framework.serializers import ModelSerializer

from mainapp.models import DataSourceMethod


class DataSourceMethodSerializer(ModelSerializer):
    class Meta:
        model = DataSourceMethod
        fields = (
            "method",
            "data_source",
            "included",
            "attributes",
            "processed_images_status",
        )

        extra_kwargs = {"method": {"read_only": True}}
