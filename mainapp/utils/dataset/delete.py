import logging

from mainapp.exceptions import BucketNotFound, PolicyNotFound, RoleNotFound
from mainapp.utils.aws_utils import delete_database
from mainapp.utils.lib import delete_bucket, delete_role_and_policy

logger = logging.getLogger(__name__)


def delete_aws_resources_for_dataset(dataset, org_name):
    try:
        delete_dataset_bucket(dataset, org_name)
        delete_dataset_role_and_policy(dataset, org_name)
        delete_dataset_glue_database(dataset, org_name)
    except BucketNotFound as e:
        logger.warning(
            f"Bucket {e.bucket_name} was not found for dataset {dataset.name}:{dataset.id} at delete bucket operation"
        )
    except PolicyNotFound as e:
        logger.warning(
            f"Policy {e.policy} was not found for dataset {dataset.name}{dataset.id} at delete bucket operation"
        )
    except RoleNotFound as e:
        logger.warning(
            f"Role {e.role} was not found for dataset {dataset.name}:{dataset.id} at delete bucket operation"
        )


def delete_dataset_role_and_policy(dataset, org_name):
    logger.info(f"Deleting role and policy for dataset {dataset.name}:{dataset.id}")
    delete_role_and_policy(bucket_name=dataset.bucket, org_name=org_name)


def delete_dataset_bucket(dataset, org_name):
    logger.info(
        f"Deleting bucket {dataset.bucket} for dataset {dataset.name}:{dataset.id}"
    )
    delete_bucket(bucket_name=dataset.bucket, org_name=org_name)


def delete_dataset_glue_database(dataset, org_name):
    logger.info(f"Deleting glue database for dataset {dataset.name}:{dataset.id}")
    delete_database(glue_database=dataset.glue_database, org_name=org_name)
