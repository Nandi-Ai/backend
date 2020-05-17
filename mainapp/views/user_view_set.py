import logging

from rest_framework.viewsets import ReadOnlyModelViewSet

from mainapp.models import User
from mainapp.serializers import UserSerializer

logger = logging.getLogger(__name__)


class UserViewSet(ReadOnlyModelViewSet):
    serializer_class = UserSerializer
    queryset = User.objects.filter(is_execution=False)
