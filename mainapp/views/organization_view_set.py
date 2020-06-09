import logging

from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from mainapp.models import Organization, OrganizationPreference
from mainapp.serializers import (
    OrganizationSerializer,
    SingleOrganizationPreferenceSerializer,
)

logger = logging.getLogger(__name__)


class OrganizationViewSet(ReadOnlyModelViewSet):
    serializer_class = OrganizationSerializer

    def get_queryset(self, **kwargs):
        return Organization.objects.all()

    @action(detail=True, methods=["get"])
    def preferences(self, request, *args, **kwargs):
        organization = self.get_object()
        preferences = OrganizationPreference.objects.filter(organization=organization)
        data = SingleOrganizationPreferenceSerializer(
            preferences, allow_null=True, many=True
        ).data
        return Response(data)
