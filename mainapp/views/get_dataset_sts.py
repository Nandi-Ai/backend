import logging

import botocore.exceptions
from rest_framework.response import Response
from rest_framework.views import APIView

from mainapp import settings
from mainapp.models import Dataset, Execution
from mainapp.utils import aws_service
from mainapp.utils.response_handler import ErrorResponse, NotFoundErrorResponse

logger = logging.getLogger(__name__)


class GetDatasetSTS(APIView):  # for frontend uploads
    # noinspection PyMethodMayBeStatic
    def get(self, request, dataset_id):
        try:
            user = (
                request.user
                if not request.user.is_execution
                else Execution.objects.get(execution_user=request.user).real_user
            )
            dataset = user.datasets.get(id=dataset_id)
        except Dataset.DoesNotExist:
            return NotFoundErrorResponse(
                f"Dataset with that dataset_id {dataset_id} does not exists"
            )

        # generate sts token so the user can upload the dataset to the bucket
        org_name = dataset.organization.name
        sts_default_provider_chain = aws_service.create_sts_client(org_name=org_name)
        # sts_default_provider_chain = aws_service.create_sts_client()

        role_name = f"lynx-dataset-{dataset.id}"
        role_to_assume_arn = (
            f"arn:aws:iam::{settings.ORG_VALUES[org_name]['ACCOUNT_NUMBER']}"
            f":role/{role_name}"
        )

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
                f"There was an error creating STS token for dataset: {dataset_id}"
            ).with_traceback(e.__traceback__)
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=error,
            )

        config = {
            "bucket": dataset.bucket,
            "aws_sts_creds": sts_response["Credentials"],
            "region": settings.ORG_VALUES[org_name]["AWS_REGION"],
        }
        return Response(config)
