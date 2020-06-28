import logging

from rest_framework.viewsets import ReadOnlyModelViewSet

from mainapp.models import Tag
from mainapp.serializers import TagSerializer

logger = logging.getLogger(__name__)


class TagViewSet(ReadOnlyModelViewSet):
    serializer_class = TagSerializer
    queryset = Tag.objects.all().order_by("name")
