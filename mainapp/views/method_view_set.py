import logging

from rest_framework import mixins, viewsets

from mainapp.models import Method
from mainapp.serializers import MethodSerializer
from mainapp.utils.permissions import IsMethodAdmin

logger = logging.getLogger(__name__)


class MethodViewSet(mixins.DestroyModelMixin, viewsets.GenericViewSet):
    serializer_class = MethodSerializer
    queryset = Method.objects.filter()
    http_method_names = ["delete", "head"]
    permission_classes = [IsMethodAdmin]
