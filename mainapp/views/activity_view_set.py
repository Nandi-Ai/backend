import logging

import dateparser
from django.core import exceptions
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

from mainapp.models import Execution, Activity
from mainapp.serializers import ActivitySerializer
from mainapp.utils.response_handler import ErrorResponse, BadRequestErrorResponse

logger = logging.getLogger(__name__)


class ActivityViewSet(ModelViewSet):
    serializer_class = ActivitySerializer
    http_method_names = ["get", "head", "post", "delete"]
    filter_fields = ("user", "dataset", "study", "type")

    def get_queryset(self):
        # all activity for all datasets that the user admins
        return Activity.objects.filter(
            dataset_id__in=[x.id for x in self.request.user.admin_datasets.all()]
        )

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        start_raw = request.GET.get("start")
        end_raw = request.GET.get("end")

        if not all([start_raw, end_raw]):
            return ErrorResponse(
                "Please provide start and end as query string params in some datetime format"
            )
        try:
            start = dateparser.parse(start_raw)
            end = dateparser.parse(end_raw)
        except exceptions.ValidationError as e:
            return ErrorResponse(f"Cannot parse this format", error=e)

        queryset = queryset.filter(ts__range=(start, end)).order_by("-ts")
        serializer = self.serializer_class(data=queryset, allow_null=True, many=True)
        serializer.is_valid()

        return Response(serializer.data)

    def create(self, request, *args, **kwargs):
        if request.user.is_execution:
            # replace execution user with real one
            execution = Execution.objects.get(execution_user=request.user)
            request.user = execution.real_user

        activity_serialized = self.serializer_class(data=request.data, allow_null=True)

        if activity_serialized.is_valid():
            # activity_data = activity_serialized.validated_data
            activity = activity_serialized.save()
            activity.user = request.user
            activity.save()
            if activity.dataset:
                logger.info(
                    f"New Activity added : {activity.id} by user {activity.user.name} on datatset {activity.dataset.name}:{activity.dataset.id} in org {activity.dataset.organization.name}"
                )
            elif activity.study:
                logger.info(
                    f"New Activity added : {activity.id} by user {activity.user.name} on study {activity.study.name}:{activity.study.id} in org {activity.study.organization.name}"
                )
            else:
                logger.warning(
                    "The Activity added does not have neither a Dataset not a Study :O"
                )
            return Response(
                self.serializer_class(activity, allow_null=True).data, status=201
            )

        else:
            return BadRequestErrorResponse(
                "Bad Request:", error=activity_serialized.errors
            )
