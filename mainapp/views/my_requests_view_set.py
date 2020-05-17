import logging

from rest_framework.viewsets import ReadOnlyModelViewSet

from mainapp.serializers import RequestSerializer

logger = logging.getLogger(__name__)


class MyRequestsViewSet(ReadOnlyModelViewSet):
    filter_fields = ("dataset", "study", "type", "state", "permission")
    serializer_class = RequestSerializer

    def get_queryset(self):
        return self.request.user.my_requests
