import logging

from rest_framework.viewsets import ReadOnlyModelViewSet
from rest_framework.response import Response

from mainapp.models import User
from mainapp.serializers import UserSerializer

logger = logging.getLogger(__name__)


class UserViewSet(ReadOnlyModelViewSet):
    serializer_class = UserSerializer
    queryset = User.objects.filter(is_execution=False)

    def list(self, request):
        if request.query_params.get("query_param"):
            query_param = request.query_params["query_param"]
            if len(query_param) < 3:
                return Response([])
            users = list(
                User.objects.filter(is_execution=False, name__contains=query_param)
                | User.objects.filter(is_execution=False, email__contains=query_param)
            )

            return Response(
                [{"id": user.id, "name": user.name or user.email} for user in users]
            )
        return Response([])
