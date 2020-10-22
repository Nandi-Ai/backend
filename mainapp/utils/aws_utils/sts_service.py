import logging

from mainapp import settings
from mainapp.utils.decorators import with_sts_client

logger = logging.getLogger(__name__)


@with_sts_client
def assume_role(boto3_client, org_name, role_name):
    role_to_assume_arn = (
        f"arn:aws:iam::{settings.ORG_VALUES[org_name]['ACCOUNT_NUMBER']}"
        f":role/{role_name}"
    )
    return boto3_client.assume_role(
        RoleArn=role_to_assume_arn, RoleSessionName="session", DurationSeconds=43200
    )
