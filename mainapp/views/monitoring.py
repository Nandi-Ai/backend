import logging

from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from mainapp.models import User, Study
from mainapp.utils.response_handler import BadRequestErrorResponse

logger = logging.getLogger(__name__)


class Monitoring(GenericAPIView):
    EVENT_REQUEST_NOTEBOOK = "request_notebook"
    EVENT_NOTEBOOK_READY = "notebook_ready"

    def post(self, request):
        event_type = request.data["event_type"]
        if not event_type:
            return BadRequestErrorResponse("Event Type not specified. Cannot log event")
        if event_type == self.EVENT_REQUEST_NOTEBOOK:
            user = User.objects.get(id=request.data.get("data")["user"])
            study = Study.objects.get(id=request.data.get("data")["study"])
            if not user or not study:
                return BadRequestErrorResponse(
                    "Data for event was not specified correctly. Cannot log event"
                )
            logger.info(
                f"User {user.display_name} from org {user.organization.name} "
                f"has requested the notebook for Study {study.name}:{study.id} "
                f"as jupyter-{study.execution.id if study.execution else ' '}"
            )
        elif event_type == self.EVENT_NOTEBOOK_READY:
            user = User.objects.get(id=request.data.get("data")["user"])
            study = Study.objects.get(id=request.data.get("data")["study"])
            load_time = request.data.get("data")["load_time"]
            if not user or not study or not load_time:
                return BadRequestErrorResponse(
                    "Data for event was not specified correctly. Cannot log event"
                )
            logger.info(
                f"Notebook jupyter-{study.execution.id if study.execution else ' '} "
                f"for Study {study.name}:{study.id} "
                f"is ready for User {user.display_name} "
                f"from org {user.organization.name} "
                f"and took {load_time}ms to load"
            )

        return Response()
