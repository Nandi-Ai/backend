import json

from mainapp.utils.decorators import with_iam_client, with_iam_resource

MAX_DURATION_IN_SECONDS = 43200


@with_iam_client
def create_role(boto3_client, org_name, role_name, assume_role_policy_document):
    boto3_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=assume_role_policy_document,
        Description="De-id location",
        MaxSessionDuration=MAX_DURATION_IN_SECONDS,
    )


@with_iam_client
def put_policy(boto3_client, org_name, role_name, policy_name, policy_document):
    boto3_client.put_role_policy(
        RoleName=role_name, PolicyName=policy_name, PolicyDocument=policy_document
    )


@with_iam_resource
def delete_role_and_policy(boto3_client, role_name, org_name):
    role = boto3_client.Role(role_name)

    for policy in role.policies.iterator():
        policy.delete()

    role.delete()


def generate_read_s3_policy(bucket, path):
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{bucket}*"],
                    "Condition": {"StringLike": {"s3:Prefix": [path]}},
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket}/{path}*"],
                },
                {
                    "Effect": "Allow",
                    "Action": ["kms:Decrypt", "kms:DescribeKey"],
                    "Resource": "*",
                },
            ],
        }
    )


def generate_admin_s3_policy(dataset_dir, dataset_bucket):
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:*"],
                    "Resource": [f"arn:aws:s3:::{dataset_bucket}*"],
                    "Condition": {"StringLike": {"s3:Prefix": [f"{dataset_dir}*"]}},
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:*"],
                    "Resource": [f"arn:aws:s3:::{dataset_bucket}/{dataset_dir}*"],
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "kms:Encrypt",
                        "kms:Decrypt",
                        "kms:DescribeKey",
                        "kms:GenerateDataKey",
                    ],
                    "Resource": "*",
                },
            ],
        }
    )
