import logging

import dateparser
from django.core import exceptions
from rest_framework.response import Response
from rest_framework.views import APIView

from mainapp.models import Study
from mainapp.utils import lib
from mainapp.utils.response_handler import (
    ErrorResponse,
    ForbiddenErrorResponse,
    NotFoundErrorResponse,
    BadRequestErrorResponse,
)

logger = logging.getLogger(__name__)


class Version(APIView):
    # noinspection PyMethodMayBeStatic
    def get(self, request):

        if "study" not in request.query_params:
            return BadRequestErrorResponse("Please provide study as qsp")

        study_id = request.query_params.get("study")

        try:
            study = request.user.studies.get(id=study_id)
        except Study.DoesNotExist as e:
            return NotFoundErrorResponse(
                f"Study {study_id} does not exists or is not permitted", error=e
            )

        start = request.GET.get("start")
        end = request.GET.get("end")

        try:
            if start:
                start = dateparser.parse(start)
            if end:
                end = dateparser.parse(end)
        except exceptions.ValidationError as e:
            return ErrorResponse(f"Can not get list_objects_version.", error=e)

        if (start and end) and not start <= end:
            return ErrorResponse("start > end")

        dataset_from_study = study.datasets.first()
        org_name = dataset_from_study.organization.name

        try:
            items = lib.list_objects_version(
                bucket=study.bucket,
                org_name=org_name,
                filter="*.ipynb",
                exclude=".*",
                start=start,
                end=end,
            )
        except Exception as e:
            return ForbiddenErrorResponse(f"Can not get list_objects_version.", error=e)
        return Response(items)
