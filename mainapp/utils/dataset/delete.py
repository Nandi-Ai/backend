import logging

from botocore.exceptions import ClientError
from mainapp.utils.aws_utils.iam_service import delete_role_and_policy
from mainapp.utils.aws_utils.glue import delete_database
from mainapp.utils.aws_utils.s3_storage import delete_directory

logger = logging.getLogger(__name__)


def delete_aws_resources_for_dataset(dataset):
    org_name = dataset.organization.name
    delete_dataset_directory(dataset, org_name)
    delete_dataset_glue_database(dataset, org_name)
    delete_dataset_role_and_policy(dataset, org_name)


def delete_dataset_directory(dataset, org_name):
    logger.info(
        f"Deleting directory {dataset.bucket_dir} in dataset bucket {dataset.bucket}"
    )
    try:
        delete_directory(
            org_name=org_name, bucket_name=dataset.bucket, directory=dataset.bucket_dir
        )
    except ClientError as e:
        logger.warning(f"Failed to delete dataset directory due to {e}")


def delete_dataset_glue_database(dataset, org_name):
    logger.info(f"Deleting glue database for dataset {dataset.name}:{dataset.id}")
    try:
        delete_database(glue_database=dataset.glue_database, org_name=org_name)
    except ClientError as e:
        logger.warning(f"Failed to delete dataset glue tables due to {e}")


def delete_dataset_role_and_policy(dataset, org_name):
    logger.info(f"Deleting role and policy for dataset {dataset.name}:{dataset.id}")
    try:
        delete_role_and_policy(role_name=dataset.iam_role, org_name=org_name)
    except ClientError as e:
        logger.warning(f"Failed to delete dataset iam role due to {e}")
