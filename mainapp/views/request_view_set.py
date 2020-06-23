import logging

from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

from mainapp.models import Request
from mainapp.serializers import RequestSerializer
from mainapp.utils.response_handler import (
    NotFoundErrorResponse,
    ConflictErrorResponse,
    BadRequestErrorResponse,
)

logger = logging.getLogger(__name__)


class RequestViewSet(ModelViewSet):
    http_method_names = ["get", "head", "post"]
    serializer_class = RequestSerializer
    filter_fields = (
        "user_requested",
        "dataset",
        "study",
        "type",
        "state",
        "permission",
    )

    def get_queryset(self):
        return self.request.user.requests_for_me

    def create(self, request, **kwargs):
        request_serialized = self.serializer_class(data=request.data, allow_null=True)

        if request_serialized.is_valid():
            request_data = request_serialized.validated_data

            if request_data["type"] == "dataset_access":
                permission_request_types = ["aggregated_access", "full_access"]

                if "dataset" not in request_data:
                    return NotFoundErrorResponse(
                        "Please mention dataset if type is dataset_access"
                    )

                if request_data["dataset"] not in request.user.datasets.filter(
                    state="private"
                ):
                    return NotFoundErrorResponse(
                        f"Can not request access for a dataset that is not private"
                    )

                dataset = request_data["dataset"]

                if "permission" not in request_data:
                    return NotFoundErrorResponse(
                        "Please mention a permission for that kind of request"
                    )

                if request_data["permission"] not in permission_request_types:
                    return NotFoundErrorResponse(
                        f"Permission must be one of: {permission_request_types}"
                    )

                # the logic validations:
                if (
                    request.user.permission(dataset) == "full_access"
                    and request_data["permission"] == "full_access"
                ):
                    return ConflictErrorResponse(
                        f"You already have {request_data['permission']} access for this dataset {dataset.name}"
                        f"with following dataset id {dataset.id}"
                    )

                if (
                    request.user.permission(dataset) == "full_access"
                    and request_data["permission"] == "aggregated_access"
                ):
                    return ConflictErrorResponse(
                        f"You already have aggregated access for this dataset {dataset.name}"
                        f"with following dataset id{dataset.id}"
                    )

                if request.user.permission(dataset) is "admin":
                    return ConflictErrorResponse(
                        f"You are already an admin of this dataset {dataset.name} "
                        f"with the following dataset id {dataset.id}. "
                        f"Your are granted with full permission"
                    )

                existing_requests = Request.objects.filter(
                    dataset=dataset,
                    type="dataset_access",
                    user_requested=request.user,
                    state="pending",
                )

                if existing_requests.filter(permission="aggregated_access"):
                    if request_data["permission"] == "aggregated_access":
                        return ConflictErrorResponse(
                            f"You already requested aggregated access for this dataset {dataset.name}"
                            f"with following dataset id {dataset.id}"
                        )
                    if request_data["permission"] == "full_access":
                        return ConflictErrorResponse(
                            f"You have already requested aggregated access for this dataset {dataset.name} "
                            f"with the following dataset id {dataset.id}."
                            "You have to wait for an admin to response your current request "
                            "before requesting full access"
                        )

                if existing_requests.filter(permission="full_access"):
                    return ConflictErrorResponse(
                        f"You have already requested full access for that dataset {dataset.name}"
                    )

                request_data["user_requested"] = request.user
                request = request_serialized.save()

                if request.dataset:
                    logger.info(
                        f"Request created {request.id} by user: {request.user.display_name} for {request.permission} "
                        f"on dataset {request.dataset.name}:{request.dataset.id} in org {request.dataset.organization.name}"
                    )
                if request.study:
                    logger.info(
                        f"Request created {request.id} by user: {request.user.display_name} for {request.permission} "
                        f"on study {request.study.name}:{request.study.id} in org {request.study.organization.name}"
                    )

                return Response(
                    self.serializer_class(request, allow_null=True).data, status=201
                )
        else:
            return BadRequestErrorResponse(
                f"Unknown request data type {request_serialized.errors}"
            )
