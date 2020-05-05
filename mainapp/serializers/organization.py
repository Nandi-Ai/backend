from rest_framework.serializers import ModelSerializer

from mainapp.models import Organization


class OrganizationSerializer(ModelSerializer):
    class Meta:
        model = Organization
        fields = ("id", "name", "logo")
