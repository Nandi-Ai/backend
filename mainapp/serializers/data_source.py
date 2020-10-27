from rest_framework.serializers import ModelSerializer, BooleanField, ValidationError

from mainapp.models import DataSource


class DataSourceSerializer(ModelSerializer):
    is_column_present = BooleanField(write_only=True)

    def is_column_present(self, obj):
        return obj.get("is_column_present")

    def get_unique_together_validators(self):
        """
        Overriding method to disable unique together checks.
        will be declared in validate
        """
        return list()

    def validate(self, attrs):
        if not self.instance:
            try:
                DataSource.objects.get(name=attrs["name"], dataset=attrs["dataset"])
            except DataSource.DoesNotExist:
                pass
            else:
                raise ValidationError(
                    {
                        "name": [
                            "You already have datasource with the same name. Please change the file name and upload again."
                        ]
                    }
                )
        return attrs

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
            "columns",
            "needs_deid",
        )
        extra_kwargs = {
            "state": {"read_only": True},
            "cohort": {"read_only": True},
            "programmatic_name": {"read_only": True},
            "children": {"read_only": True},
        }
