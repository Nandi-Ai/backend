import logging

import botocore.exceptions
from rest_framework.response import Response
from rest_framework.views import APIView

from mainapp import settings
from mainapp.utils import aws_service
from mainapp.utils.response_handler import ErrorResponse

logger = logging.getLogger(__name__)


class GetStaticSTS(APIView):  # from execution
    # noinspection PyMethodMayBeStatic
    def get(self, request):
        sts_default_provider_chain = aws_service.create_sts_client()
        static_bucket_name = settings.LYNX_FRONT_STATIC_BUCKET
        role_to_assume_arn = (
            f"arn:aws:iam::{settings.ORG_VALUES['Lynx MD']['ACCOUNT_NUMBER']}:role/"
            f"{settings.AWS_STATIC_ROLE_NAME}"
        )

        try:
            response = sts_default_provider_chain.assume_role(
                RoleArn=role_to_assume_arn, RoleSessionName="session"
            )
        except botocore.exceptions.ClientError as e:
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=e,
            )
        except Exception as e:
            error = Exception(
                f"The server can't process your request due to unexpected internal error"
            ).with_traceback(e.__traceback__)
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=error,
            )
        logger.info(f"Generated STS credentials for static bucket in org Lynx MD")
        config = {
            "bucket": static_bucket_name,
            "aws_sts_creds": response["Credentials"],
        }

        return Response(config)
