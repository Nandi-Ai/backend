import logging

import botocore.exceptions
from rest_framework.response import Response
from rest_framework.views import APIView

from mainapp import settings
from mainapp.models import Dataset
from mainapp.utils import aws_service
from mainapp.utils.response_handler import ErrorResponse, NotFoundErrorResponse

logger = logging.getLogger(__name__)


class GetStudySTS(APIView):  # for frontend uploads
    # noinspection PyMethodMayBeStatic
    def get(self, request, study_id):
        try:
            study = request.user.studies.get(id=study_id)
        except Dataset.DoesNotExist as e:
            raise NotFoundErrorResponse(
                f"Study with that id {study_id} does not exists"
            ) from e

        # generate sts token so the user can upload the dataset to the bucket
        org_name = study.organization.name

        sts_default_provider_chain = aws_service.create_sts_client(org_name=org_name)

        role_name = f"lynx-workspace-{study.id}"
        role_to_assume_arn = f"arn:aws:iam::{settings.ORG_VALUES[org_name]['ACCOUNT_NUMBER']}:role/{role_name}"

        try:
            sts_response = sts_default_provider_chain.assume_role(
                RoleArn=role_to_assume_arn,
                RoleSessionName="session",
                DurationSeconds=43200,
            )
        except botocore.exceptions.ClientError as e:
            error = Exception(
                f"The server can't process your request due to unexpected internal error"
            ).with_traceback(e.__traceback__)
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=error,
            )
        except Exception as e:
            error = Exception(
                f"There was an error creating a STS token for this study: {study.id}"
            ).with_traceback(e.__traceback__)
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=error,
            )
        logger.info(
            f"Generated STS credentials for Study: {study.name}:{study.id} in org {study.organization.name}"
        )
        config = {
            "bucket": study.bucket,
            "aws_sts_creds": sts_response["Credentials"],
            "region": settings.ORG_VALUES[org_name]["AWS_REGION"],
        }

        return Response(config)
