import logging

from rest_framework.response import Response
from rest_framework.views import APIView

from mainapp.models import Request
from mainapp.utils.response_handler import NotFoundErrorResponse

logger = logging.getLogger(__name__)


class HandleDatasetAccessRequest(APIView):
    def get(self, request, user_request_id):
        possible_responses = ["approve", "deny"]
        response = request.query_params.get("response")

        if response not in possible_responses:
            return NotFoundErrorResponse(
                f"Please response with query string param: {possible_responses}"
            )

        try:
            user_request = self.request.user.requests_for_me.get(id=user_request_id)
        except Request.DoesNotExist:
            return NotFoundErrorResponse("Request not found")

        user_request.state = "approved" if response is "approve" else "denied"
        user_request.save()

        return Response()
