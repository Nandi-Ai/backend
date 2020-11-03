import json
from mainapp.utils.aws_utils import iam_service
from botocore.exceptions import ClientError
from mainapp.resources import (
    generate_dataset_permission_access_policy,
    create_base_trust_relationship,
)

from mainapp.exceptions import CreateRoleError, PutPolicyError


def create_dataset_permission_access_role(org_name, role_name, location, bucket):
    trust_policy_json = create_base_trust_relationship(org_name=org_name)

    try:
        iam_service.create_role(
            org_name=org_name,
            role_name=role_name,
            assume_role_policy_document=json.dumps(trust_policy_json),
        )
    except ClientError as e:
        raise CreateRoleError(role_name=role_name, error=e)

    policy_json = generate_dataset_permission_access_policy(
        bucket=bucket, location=location
    )

    try:
        iam_service.put_policy(
            org_name=org_name,
            role_name=role_name,
            policy_name=role_name,
            policy_document=json.dumps(policy_json),
        )
    except ClientError as e:
        raise PutPolicyError(policy_name=role_name, role_name=role_name, error=e)
