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
        def inner(org_name, *args, **kwargs):
            if not org_name:
                raise ValueError("org_name param is empty")
            client_instance = client(org_name=org_name)
            return func(
                boto3_client=client_instance, org_name=org_name, *args, **kwargs
            )

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
