import logging

from botocore.exceptions import ClientError
from multiprocessing import Manager

from mainapp.settings import ORG_VALUES
from mainapp.utils.decorators import with_storage_gateway_client


logger = logging.getLogger(__name__)
cache_rlock = Manager().RLock()


@with_storage_gateway_client
def refresh_dataset_file_share_cache(boto3_client, org_name):
    logger.info(f"Refreshing File Share Cache for organization {org_name}")
    try:
        with cache_rlock:
            boto3_client.refresh_cache(
                FileShareARN=ORG_VALUES[org_name]["FILE_SHARE_ARN"]
            )
    except ClientError:
        logger.warning(f"Failed to refresh cache for {org_name} File Share")
