import logging

import botocore.exceptions
from rest_framework.response import Response
from rest_framework.views import APIView

from mainapp import settings
from mainapp.models import Study
from mainapp.utils import aws_service
from mainapp.utils.response_handler import ErrorResponse

logger = logging.getLogger(__name__)


class GetSTS(APIView):  # from execution
    # noinspection PyMethodMayBeStatic
    def get(self, request):

        execution = request.user.the_execution.last()
        # service = request.query_params.get('service')

        try:
            study = Study.objects.filter(execution=execution).last()
        except Study.DoesNotExist:
            return ErrorResponse("This is not the execution of any study")

        # Create IAM client
        # request user is the execution user
        sts_default_provider_chain = aws_service.create_sts_client(
            org_name=request.user.organization.name
        )

        workspace_bucket_name = study.bucket

        org_name = request.user.organization.name

        role_to_assume_arn = f"arn:aws:iam::{settings.ORG_VALUES[org_name]['ACCOUNT_NUMBER']}:role/{workspace_bucket_name}"

        try:
            response = sts_default_provider_chain.assume_role(
                RoleArn=role_to_assume_arn, RoleSessionName="session"
            )
        except botocore.exceptions.ClientError as e:
            error = Exception(
                f"Error calling 'assume_role' in 'GetSTS' for study {study.id}, organization: {org_name}"
            ).with_traceback(e.__traceback__)
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=error,
            )
        except Exception as e:
            error = Exception(
                f"There was an error when requesting STS credentials for study {study.id}"
            ).with_traceback(e.__traceback__)
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=error,
            )

        config = {
            "bucket": workspace_bucket_name,
            "aws_sts_creds": response["Credentials"],
            "region": settings.ORG_VALUES[org_name]["AWS_REGION"],
        }

        return Response(config)
