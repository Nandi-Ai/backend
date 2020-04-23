from rest_framework.serializers import ModelSerializer

from mainapp.models import DataSource


class DataSourceSerializer(ModelSerializer):
    class Meta:
        model = DataSource
        fields = (
            "id",
            "name",
            "dir",
            "s3_objects",
            "type",
            "about",
            "programmatic_name",
            "dataset",
            "state",
            "glue_table",
            "children",
            "ancestor",
            "cohort",
        )
        extra_kwargs = {
            "state": {"read_only": True},
            "cohort": {"read_only": True},
            "programmatic_name": {"read_only": True},
            "children": {"read_only": True},
        }
