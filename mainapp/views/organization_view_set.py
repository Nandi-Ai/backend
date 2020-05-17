import logging

from rest_framework.viewsets import ReadOnlyModelViewSet

from mainapp.models import Organization
from mainapp.serializers import OrganizationSerializer

logger = logging.getLogger(__name__)


class OrganizationViewSet(ReadOnlyModelViewSet):
    serializer_class = OrganizationSerializer

    def get_queryset(self, **kwargs):
        return Organization.objects.all()
