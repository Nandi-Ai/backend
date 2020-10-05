from django.db import transaction
from rest_framework.exceptions import ValidationError
from rest_framework.serializers import ModelSerializer

from mainapp.models import Method, DataSourceMethod
from mainapp.serializers.data_source_method import DataSourceMethodSerializer


class MethodSerializer(ModelSerializer):
    data_source_methods = DataSourceMethodSerializer(many=True)

    class Meta:
        model = Method
        fields = (
            "id",
            "name",
            "dataset",
            "data_source_methods",
            "group_age_over",
            "state",
            "updated_at",
            "created_at",
        )

    def get_unique_together_validators(self):
        """
        Overriding method to disable unique together checks.
        will be declared in validate
        """
        return []

    def validate(self, attrs):
        try:
            Method.objects.get(name=attrs["name"], dataset=attrs["dataset"])
        except Method.DoesNotExist:
            pass
        else:
            raise ValidationError({"name": ["method with same name already exists"]})

        return attrs

    @transaction.atomic
    def create(self, validated_data):
        data_source_methods = validated_data.pop("data_source_methods")
        method = Method.objects.create(**validated_data)
        for data_source_method in data_source_methods:
            if data_source_method["included"]:
                DataSourceMethod.objects.create(method=method, **data_source_method)
        return method
