import logging

from rest_framework.response import Response
from rest_framework import mixins, viewsets

from mainapp.models import User
from mainapp.serializers import UserSerializer
from mainapp.utils.response_handler import ForbiddenErrorResponse

logger = logging.getLogger(__name__)


class UserViewSet(
    mixins.RetrieveModelMixin,
    mixins.UpdateModelMixin,
    mixins.ListModelMixin,
    viewsets.GenericViewSet,
):

    serializer_class = UserSerializer
    queryset = User.objects.filter(is_execution=False, is_active=True)
    http_method_names = ["get", "put", "head"]

    def list(self, request):
        if request.query_params.get("query_param"):
            query_param = request.query_params["query_param"]
            if len(query_param) < 3:
                return Response([])
            queryset = self.get_queryset()
            users = list(
                queryset.filter(name__icontains=query_param)
                | queryset.filter(email__icontains=query_param)
            )

            return Response(
                [{"id": user.id, "name": user.name or user.email} for user in users]
            )
        return Response([])

    def update(self, request, *args, **kwargs):
        if self.get_object().id != request.user.id:
            return ForbiddenErrorResponse(f"User is not the same {request.user.id}")
        return super(self.__class__, self).update(request=self.request)
