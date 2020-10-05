import logging

from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from mainapp.utils.monitoring import handle_event
from mainapp.utils.response_handler import BadRequestErrorResponse
from mainapp.models import Study, User

logger = logging.getLogger(__name__)


class Monitoring(GenericAPIView):
    def post(self, request):
        event_type = request.data["event_type"]
        if not event_type:
            return BadRequestErrorResponse("Event Type not specified. Cannot log event")

        request_args = {}
        if "user" in request.data.get("data"):
            request_args["user"] = User.objects.get(id=request.data.get("data")["user"])
        if "study" in request.data.get("data"):
            request_args["study"] = Study.objects.get(
                id=request.data.get("data")["study"]
            )

        if request_args:
            request_args["data"] = request.data.get("data")

        handle_event(event_type, request_args or request.data)
        return Response()
