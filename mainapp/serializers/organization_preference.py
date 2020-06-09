from rest_framework.serializers import ModelSerializer

from mainapp.models import OrganizationPreference


class OrganizationPreferenceSerializer(ModelSerializer):
    class Meta:
        model = OrganizationPreference
        fields = ("organization", "key", "value")


class SingleOrganizationPreferenceSerializer(ModelSerializer):
    class Meta:
        model = OrganizationPreference
        fields = ("key", "value")
