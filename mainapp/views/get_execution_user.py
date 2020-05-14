import logging

from rest_framework.response import Response
from rest_framework.views import APIView

from mainapp.models import Execution
from mainapp.utils.response_handler import BadRequestErrorResponse

logger = logging.getLogger(__name__)


class GetExecutionUser(APIView):
    def get(self, request):
        try:
            execution = Execution.objects.get(execution_user=request.user)
        except Execution.DoesNotExist:
            return BadRequestErrorResponse(f"{request.user} is not an execution user")

        return Response({"user": execution.real_user.id})
