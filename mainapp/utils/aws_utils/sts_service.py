import logging

from mainapp import settings
from mainapp.utils.aws_utils.iam_service import MAX_DURATION_IN_SECONDS
from mainapp.utils.decorators import with_sts_client


logger = logging.getLogger(__name__)


@with_sts_client
def assume_role(boto3_client, org_name, role_name, policy=None):
    role_to_assume = {
        "RoleArn": f"arn:aws:iam::{settings.ORG_VALUES[org_name]['ACCOUNT_NUMBER']}:role/{role_name}",
        "RoleSessionName": "session",
        "DurationSeconds": MAX_DURATION_IN_SECONDS,
    }

    if policy:
        role_to_assume["Policy"] = policy

    return boto3_client.assume_role(**role_to_assume)
