import logging
import math

from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

from mainapp.models import Request
from mainapp.serializers import RequestSerializer
from mainapp.utils.response_handler import (
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
                permission_request_types = [
                    "aggregated_access",
                    "full_access",
                    "limited_access",
                    "deid_access",
                ]

                if "dataset" not in request_data:
                    return BadRequestErrorResponse(
                        "Please mention dataset if type is dataset_access"
                    )

                if request_data["dataset"] not in request.user.datasets.filter(
                    state="private"
                ):
                    return BadRequestErrorResponse(
                        f"Can not request access for a dataset that is not private"
                    )

                dataset = request_data["dataset"]

                if "permission" not in request_data:
                    return BadRequestErrorResponse(
                        "Please mention a permission for that kind of request"
                    )

                requested_permission = request_data["permission"]

                if requested_permission not in permission_request_types:
                    return BadRequestErrorResponse(
                        f"Permission must be one of: {permission_request_types}"
                    )

                if requested_permission == "limited_access":
                    if "permission_attributes" not in request_data:
                        return BadRequestErrorResponse(
                            f"permission_attributes field is required for Limited access request"
                        )
                    permission_attributes = request_data["permission_attributes"]
                    if "key" not in permission_attributes:
                        return BadRequestErrorResponse(
                            f"key field is missing in permission_attributes"
                        )
                    key = permission_attributes["key"]
                    if not math.isnan(key) and not key > 0:
                        return BadRequestErrorResponse(
                            f"Limited value must be valid positive whole number: {permission_request_types}"
                        )

                if requested_permission == "deid_access":
                    if "permission_attributes" in request_data:
                        return BadRequestErrorResponse(
                            f"requesting de-id can't be with permission_attributes"
                        )

                # the logic validations:
                current_user_permission = request.user.permission(dataset)

                if (
                    current_user_permission == "full_access"
                    and requested_permission == "full_access"
                ):
                    return ConflictErrorResponse(
                        f"You already have {request_data['permission']} access for this dataset {dataset.name}"
                        f"with following dataset id {dataset.id}"
                    )

                if (
                    current_user_permission == "full_access"
                    and requested_permission == "aggregated_access"
                ):
                    return ConflictErrorResponse(
                        f"You already have aggregated access for this dataset {dataset.name}"
                        f"with following dataset id{dataset.id}"
                    )

                if (
                    current_user_permission == "full_access"
                    and requested_permission == "limited_access"
                ):
                    return ConflictErrorResponse(
                        f"You already have full access for this dataset {dataset.name}"
                        f"with following dataset id{dataset.id}"
                    )

                if current_user_permission is "admin":
                    return ConflictErrorResponse(
                        f"You are already an admin of this dataset {dataset.name} "
                        f"with the following dataset id {dataset.id}. "
                        f"You are granted with full permission"
                    )

                existing_requests = Request.objects.filter(
                    dataset=dataset,
                    type="dataset_access",
                    user_requested=request.user,
                    state="pending",
                )

                if existing_requests.filter(permission="aggregated_access"):
                    if requested_permission == "aggregated_access":
                        return ConflictErrorResponse(
                            f"You already requested aggregated access for this dataset {dataset.name}"
                            f"with following dataset id {dataset.id}"
                        )
                    if requested_permission == "full_access":
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

                if existing_requests.filter(permission="limited_access"):
                    if requested_permission == "limited_access":
                        return ConflictErrorResponse(
                            f"You already requested limited access for this dataset {dataset.name}"
                            f"with following dataset id {dataset.id}"
                        )
                    if requested_permission == "full_access":
                        return ConflictErrorResponse(
                            f"You have already requested limited access for this dataset {dataset.name} "
                            f"with the following dataset id {dataset.id}."
                            "You have to wait for an admin to response your current request "
                            "before requesting full access"
                        )

                request_data["user_requested"] = request.user
                created_request = request_serialized.save()

                if created_request.dataset:
                    logger.info(
                        f"Request created {created_request.id} by user: {created_request.user_requested.display_name} "
                        f"for {created_request.permission} "
                        f"on dataset {created_request.dataset.name}:{created_request.dataset.id} "
                        f"in org {created_request.dataset.organization.name}"
                    )
                if created_request.study:
                    logger.info(
                        f"Request created {created_request.id} by user: {created_request.user_requested.display_name} "
                        f"for {created_request.permission} "
                        f"on study {created_request.study.name}:{created_request.study.id} "
                        f"in org {created_request.study.organization.name}"
                    )

                return Response(
                    self.serializer_class(created_request, allow_null=True).data,
                    status=201,
                )
        else:
            return BadRequestErrorResponse(
                f"Unknown request data type {request_serialized.errors}"
            )
