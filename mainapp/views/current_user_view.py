import logging

from rest_framework.response import Response
from rest_framework.views import APIView

from mainapp.serializers import UserSerializer

logger = logging.getLogger(__name__)


class CurrentUserView(APIView):
    # noinspection PyMethodMayBeStatic
    def get(self, request):
        serializer = UserSerializer(request.user)
        return Response(serializer.data)
