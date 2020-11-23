from mainapp import settings
from mainapp.exceptions import InvalidOrganizationSettings, InvalidOrganizationOrgValues
from mainapp.exceptions import MissingOrganizationSettingKey
from mainapp.utils import aws_service


def organization_dependent(func):
    def inner(org_name="Lynx MD", user=None, *args, **kwargs):
        try:
            org_values = settings.ORG_VALUES
        except AttributeError as e:
            raise InvalidOrganizationOrgValues from e
        org_settings = org_values.get(
            (user and user.organization and user.organization.name) or org_name
        )
        if not org_settings:
            raise InvalidOrganizationSettings(org_name)

        if "AWS_ACCESS_KEY_ID" not in org_settings:
            raise MissingOrganizationSettingKey(org_name, "AWS_ACCESS_KEY_ID")

        if "AWS_SECRET_ACCESS_KEY" not in org_settings:
            raise MissingOrganizationSettingKey(org_name, "AWS_SECRET_ACCESS_KEY")

        return func(org_settings=org_settings, org_name=org_name, *args, **kwargs)

    return inner


def with_client(client):
    def decorator(func):
        def inner(*args, **kwargs):
            boto3_client = kwargs.pop("boto3_client", None)
            org_name = kwargs.get("org_name", None)
            if not org_name and not boto3_client:
                raise ValueError("org_name param and boto3_client are both empty")
            if not boto3_client:
                boto3_client = client(org_name=org_name)
            return func(boto3_client=boto3_client, *args, **kwargs)

        return inner

    return decorator


def with_glue_client(func):
    return with_client(client=aws_service.create_glue_client)(func)


def with_athena_client(func):
    return with_client(client=aws_service.create_athena_client)(func)


def with_ec2_client(func):
    return with_client(client=aws_service.create_ec2_client)(func)


def with_ec2_resource(func):
    return with_client(client=aws_service.create_ec2_resource)(func)


def with_s3_client(func):
    return with_client(client=aws_service.create_s3_client)(func)


def with_s3_resource(func):
    return with_client(client=aws_service.create_s3_resource)(func)


def with_iam_resource(func):
    return with_client(client=aws_service.create_iam_resource)(func)


def with_route53_client(func):
    return with_client(client=aws_service.create_route53_client)(func)


def with_sts_client(func):
    return with_client(client=aws_service.create_sts_client)(func)


def with_iam_client(func):
    return with_client(client=aws_service.create_iam_client)(func)


def with_storage_gateway_client(func):
    return with_client(client=aws_service.create_storage_gateway_client)(func)
