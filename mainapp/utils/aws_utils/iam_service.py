from mainapp.utils.decorators import with_iam_client

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
