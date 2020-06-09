import boto3
import logging

from botocore.client import Config

from .decorators import organization_dependent

logger = logging.getLogger(__name__)


@organization_dependent
def create_client(org_settings, org_name, service_name, *args, **kwargs):
    logger.debug(f"Creation of {service_name} client for organization {org_name}")
    return boto3.client(
        service_name,
        region_name=org_settings["AWS_REGION"],
        aws_access_key_id=org_settings["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=org_settings["AWS_SECRET_ACCESS_KEY"],
        *args,
        **kwargs,
    )


@organization_dependent
def create_resource(org_settings, org_name, service_name, *args, **kwargs):
    logger.debug(f"Creation of {service_name} resource for organization {org_name}")
    return boto3.resource(
        service_name,
        region_name=org_settings["AWS_REGION"],
        aws_access_key_id=org_settings["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=org_settings["AWS_SECRET_ACCESS_KEY"],
    )


def create_sts_client(*args, **kwargs):
    return create_client(service_name="sts", *args, **kwargs)


def create_athena_client(*args, **kwargs):
    return create_client(service_name="athena", *args, **kwargs)


def create_s3_client(config=Config(signature_version="s3v4"), *args, **kwargs):
    return create_client(service_name="s3", config=config, *args, **kwargs)


def create_iam_client(*args, **kwargs):
    return create_client(service_name="iam", *args, **kwargs)


def create_glue_client(*args, **kwargs):
    return create_client(service_name="glue", *args, **kwargs)


def create_kms_client(*args, **kwargs):
    return create_client(service_name="kms", *args, **kwargs)


def create_s3_resource(*args, **kwargs):
    return create_resource(service_name="s3", *args, **kwargs)


def create_iam_resource(*args, **kwargs):
    return create_resource(service_name="iam", *args, **kwargs)
