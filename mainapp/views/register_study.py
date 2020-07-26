import logging

from rest_framework.response import Response
from rest_framework.views import APIView

from mainapp.models import Study, Execution, User
from mainapp.utils.response_handler import ErrorResponse, NotFoundErrorResponse
from mainapp.utils.study_vm_service import change_resource_record_sets
from mainapp.utils.aws_utils.route_53 import Route53Actions
from mainapp.exceptions import Route53Error


logger = logging.getLogger(__name__)


class RegisterStudy(APIView):
    """View to register the new study's DNS record"""

    def post(self, request):
        try:
            study = Study.objects.get(
                execution=Execution.objects.get(
                    execution_user=User.objects.get(email=request.user.email)
                )
            )
            if study.status == Study.STUDY_DELETED:
                return NotFoundErrorResponse(f"Study has been deleted")

            try:
                change_resource_record_sets(
                    execution=study.execution.execution_user.email,
                    org_name=study.organization.name,
                    action=Route53Actions.CREATE,
                )

            except Route53Error as ex:
                logger.error(
                    f"unable to register study  {study.id} due to Error: '{ex}'"
                )
                raise

            return Response(status=201)

        except Study.DoesNotExist:
            return NotFoundErrorResponse(f"Study does not exist")
        except Exception as ex:
            return ErrorResponse(
                "An Error occurred when trying to register the DNS", ex
            )
