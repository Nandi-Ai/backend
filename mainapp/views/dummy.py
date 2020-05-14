import logging

from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class Dummy(APIView):  # usage in Lambda Function
    # noinspection PyMethodMayBeStatic
    def get(self, request):
        return Response()
