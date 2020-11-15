import logging
import os
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework import mixins, viewsets

from mainapp import settings
from mainapp.models import User
from mainapp.serializers import UserSerializer
from mainapp.utils import aws_service, lib
from mainapp.utils.response_handler import (
    ForbiddenErrorResponse,
    BadRequestErrorResponse,
)
from mainapp.utils.monitoring import handle_event, MonitorEvents

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
    file_types = {
        ".jpg": ["image/jpeg"],
        ".jpeg": ["image/jpeg"],
        ".tiff": ["image/tiff"],
        ".png": ["image/png"],
        ".bmp": ["image/bmp"],
    }

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
                [{"id": user.id, "display_name": user.display_name} for user in users]
            )
        return Response([])

    def update(self, request, *args, **kwargs):
        user = self.get_object()
        if user.id != request.user.id:
            return ForbiddenErrorResponse(f"User is not the same {request.user.id}")
        if user.photo != request.data["photo"]:
            file_name = request.data["photo"]
            workdir = "/tmp/"
            s3_client = aws_service.create_s3_client(
                org_name=settings.LYNX_ORGANIZATION
            )
            local_path = os.path.join(workdir, file_name)
            try:
                lib.validate_file_type(
                    s3_client=s3_client,
                    bucket=settings.LYNX_FRONT_STATIC_BUCKET,
                    workdir="/tmp/user/",
                    object_key=file_name,
                    local_path=local_path,
                    file_types=self.file_types,
                )
            except Exception as e:
                return BadRequestErrorResponse(str(e))
        return super(self.__class__, self).update(request=self.request)

    @action(detail=True, methods=["put"])
    def update_agreed_eula(self, request, *args, **kwargs):
        user = self.get_object()
        user.agreed_eula_file_path = settings.EULA_FILE_PATH
        user.save()

        handle_event(
            MonitorEvents.EVENT_USER_SIGNED_EULA,
            {"user": user, "view_request": request},
        )

        return Response(UserSerializer(user).data, status=200)
