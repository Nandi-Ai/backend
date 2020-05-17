import logging
import logging
import uuid

from django.db import transaction
from rest_framework.response import Response
from rest_framework.views import APIView

from mainapp.models import User, Study, Execution
from mainapp.utils.response_handler import ForbiddenErrorResponse, NotFoundErrorResponse

logger = logging.getLogger(__name__)


class GetExecution(APIView):  # from frontend
    @transaction.atomic
    # noinspection PyMethodMayBeStatic
    def get(self, request):
        study_id = request.query_params.get("study")

        try:
            study = request.user.studies.get(id=study_id)
        except Study.DoesNotExist:
            return NotFoundErrorResponse(f"Study {study_id} does not exists")

        if request.user not in study.users.all():
            return ForbiddenErrorResponse(
                f"Only users that have this study {study_id} can get a study execution"
            )

        if not study.execution:
            execution_id = uuid.uuid4()

            # headers = {
            # "Authorization": "Bearer " + settings['JH_API_ADMIN_TOKEN'], "ALBTOKEN": settings['JH_ALB_TOKEN']
            # }
            #
            # data = {
            #     "usernames": [
            #         str(id).split("-")[-1]
            #     ],
            #     "admin": False
            # }
            # res = requests.post(settings.jh_url + "hub/api/users", json=data, headers=headers, verify=False)
            # if res.status_code != 201:
            #     return Error(
            #     "error creating a user for the execution in JH: " + str(res.status_code) + ", " + res.text
            #     )

            # execution.study = study
            execution = Execution.objects.create(id=execution_id)
            execution.real_user = request.user
            execution_user = User.objects.create_user(
                email=execution.token + "@lynx.md"
            )
            execution_user.set_password(execution.token)
            execution_user.organization = study.datasets.first().organization
            execution_user.is_execution = True
            execution_user.save()
            execution.execution_user = execution_user
            execution.save()
            study.execution = execution
            study.save()

        return Response({"execution_identifier": str(study.execution.token)})
