import logging

from rest_framework.response import Response
from rest_framework.views import APIView

from mainapp.exceptions import InvalidEc2Status
from mainapp.models import Execution, Study
from mainapp.utils.response_handler import (
    BadRequestErrorResponse,
    ForbiddenErrorResponse,
)
from mainapp.utils.study_vm_service import (
    toggle_study_vm,
    update_study_state,
    STATUS_ARGS,
)

logger = logging.getLogger(__name__)


class UpdateStudyStatus(APIView):
    """View to allow study status updates externally"""

    def post(self, request):
        try:
            update_study_state(request.user.email, request.data.get("status"))
            return Response(status=201)
        except InvalidEc2Status as ex:
            return BadRequestErrorResponse(message=str(ex))


class ToggleStudyInstance(APIView):
    def get(self, request, study_id):
        try:
            study = Study.objects.get(id=study_id)
            if request.user not in study.users.all():
                return ForbiddenErrorResponse(
                    f"Only the study creator can edit a study"
                )

            status = request.query_params.get("action")

            if status not in STATUS_ARGS:
                raise InvalidEc2Status(status)
            execution_user = Execution.objects.get(real_user=request.user)
            logger.info(
                f"Changing study {study_id} ({study.name}) instance {execution_user.email} state to {status}"
            )

            toggle_study_vm(
                execution=execution_user.email,
                org_name=study.organization.name,
                **STATUS_ARGS[status],
            )
            return Response(status=201)
        except InvalidEc2Status as ex:
            return BadRequestErrorResponse(message=str(ex))
        except:
            return BadRequestErrorResponse(message=str(ex))
