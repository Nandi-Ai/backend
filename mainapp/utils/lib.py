import csv
import json
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import zipfile
from datetime import datetime as dt, timedelta as td
from enum import Enum
from time import sleep

import botocore
import botocore.exceptions
import magic
import sqlparse
from rest_framework.authentication import TokenAuthentication
from rest_framework.exceptions import AuthenticationFailed

from mainapp import models
from mainapp import settings
from mainapp.exceptions import (
    BucketNotFound,
    UnableToGetGlueColumns,
    RoleNotFound,
    PolicyNotFound,
    UnsupportedColumnTypeError,
    MaxExecutionReactedError,
    InvalidExecutionId,
    QueryExecutionError,
)
from mainapp.exceptions.s3 import TooManyBucketsException
from mainapp.utils import aws_service, statistics, devexpress_filtering, executor
from mainapp.utils.aws_utils import s3_storage
from mainapp.utils.decorators import (
    organization_dependent,
    with_glue_client,
    with_s3_client,
    with_s3_resource,
    with_iam_resource,
    with_athena_client,
)
from mainapp.utils.response_handler import (
    ForbiddenErrorResponse,
    ErrorResponse,
    UnimplementedErrorResponse,
)

logger = logging.getLogger(__name__)


class PrivilegePath(Enum):
    FULL = "full_access"
    AGG_STATS = "aggregated_access"
    LIMITED = "limited_access"


LYNX_STORAGE_DIR = "lynx-storage"
UNSUPPORTED_CHARS = [".", ",", ":", "[", "]"]


def break_s3_object(obj):
    file_name = obj.split("/")[-1]
    file_name_no_ext = ".".join(file_name.split(".")[:-1])
    ext = file_name.split(".")[-1]
    path = "/".join(obj.split("/")[:-1])

    return path, file_name, file_name_no_ext, ext


def validate_file_type(s3_client, bucket, workdir, object_key, local_path, file_types):
    try:
        os.makedirs(workdir)
        s3_client.download_file(bucket, object_key, local_path)
        extension = os.path.splitext(local_path)[1]
        mime_by_content = magic.from_file(local_path, mime=True)
        assert all([mime_by_content, extension]) and mime_by_content in file_types.get(
            extension
        )
    except AssertionError:
        s3_client.delete_object(Bucket=bucket, Key=object_key)
        raise
    finally:
        shutil.rmtree(workdir)


@with_s3_resource
def check_csv_for_empty_columns(boto3_client, org_name, data_source):
    s3_obj = data_source.s3_objects[0]["key"]
    s3_file = boto3_client.Object(data_source.dataset.bucket, s3_obj)

    column_line = list(s3_file.get(Range="bytes=1e+6")["Body"].iter_lines())[0].decode(
        "utf-8"
    )

    download_and_upload_fixed_file(
        org_name=org_name,
        column_line=column_line,
        data_source=data_source,
        s3_obj=s3_obj,
    )


@with_s3_client
def download_and_upload_fixed_file(
    boto3_client, org_name, column_line, data_source, s3_obj
):
    delimiter = csv.Sniffer().sniff(column_line).delimiter
    bucket_name = data_source.dataset.bucket
    path, file_name, _, _ = break_s3_object(s3_obj)

    is_unsupported_char_present = check_for_unsupported(column_line)

    if (
        column_line[0] == delimiter
        or column_line[-1] == delimiter
        or f"{delimiter}{delimiter}" in column_line
        or is_unsupported_char_present
    ):
        temp_dir = tempfile.TemporaryDirectory(str(data_source.id))
        file_path = os.path.join(temp_dir.name, file_name)
        temp_file_path = f"{file_path}.temp"

        try:
            s3_storage.download_file(
                s3_client=boto3_client,
                bucket_name=bucket_name,
                s3_object=s3_obj,
                file_path=temp_file_path,
            )

            replace_col_name_on_downloaded_file(temp_file_path, file_path, delimiter)

            s3_storage.upload_file(
                s3_client=boto3_client,
                csv_path_and_file=file_path,
                bucket_name=bucket_name,
                file_path=f"{path}/{file_name}",
            )

        except boto3_client.exceptions as e:
            logger.error(e)
            logger.exception(
                f"Failed to download/upload file {file_name} from s3 bucket {bucket_name} in org {org_name}"
            )
            raise
        finally:
            temp_dir.cleanup()


def check_for_unsupported(column_line):
    list_column_line_data = column_line.split(",")

    for item in list_column_line_data:
        if any(char in item for char in UNSUPPORTED_CHARS):
            return True
    return False


def replace_col_name_on_downloaded_file(read_file_path, write_file_path, delimiter):
    with open(read_file_path, "r") as read_file, open(
        write_file_path, "w"
    ) as write_file:
        split_line = read_file.readline().split(delimiter)
        for index, item in enumerate(split_line):
            if not item:
                split_line[index] = f"Col{index}"

            unsupported_char = [char for char in UNSUPPORTED_CHARS if char in item]
            if unsupported_char:
                split_line[index] = item.replace(unsupported_char[0], " ")
        write_file.write(",".join(split_line))
        write_file.write(read_file.read())


@organization_dependent
def create_s3_bucket(
    org_settings,
    org_name,
    name,
    s3_client=None,
    encrypt=settings.SECURED_BUCKET,
    https_only=settings.SECURED_BUCKET,
):
    if not s3_client:
        s3_client = aws_service.create_s3_client(org_name=org_name)
    # https://github.com/boto/boto3/issues/125
    args = {"Bucket": name, "ACL": "private"}
    if not org_settings["AWS_REGION"] == "us-east-1":
        args["CreateBucketConfiguration"] = {
            "LocationConstraint": org_settings["AWS_REGION"]
        }
    try:
        s3_client.create_bucket(**args)
    except s3_client.exceptions.TooManyBuckets as e:
        raise TooManyBucketsException() from e

    try:
        response = s3_client.put_public_access_block(
            Bucket=name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
    except BucketNotFound as e:
        raise BucketNotFound(
            f"The bucket queried does not exist. Bucket: {name}, in org {org_name}"
        ) from e
    except s3_client.exceptions.ClientError as e:
        raise ForbiddenErrorResponse(
            f"Missing s3:PutBucketPublicAccessBlock permissions to put public access block policy for bucket: "
            f"{name}, in org {org_name}",
            e,
        )
    except Exception as e:
        raise Exception(
            f"There was an error when removing public access from bucket: {name} in org {org_name}",
            e,
        )

    s3_resource = aws_service.create_s3_resource(org_name)
    bucket_versioning = s3_resource.BucketVersioning(name)
    bucket_versioning.enable()

    if encrypt:
        logger.debug(f"Creating encrypted bucket: {name} in org {org_name}")
        kms_client = aws_service.create_kms_client(org_name=org_name)
        try:
            response = kms_client.describe_key(KeyId="alias/aws/s3")
        except Exception as e:
            raise Exception(
                f"Error occurred while creating kms key for encryption bucket {name}"
            ) from e

        try:
            s3_client.put_bucket_encryption(
                Bucket=name,
                ServerSideEncryptionConfiguration={
                    "Rules": [
                        {
                            "ApplyServerSideEncryptionByDefault": {
                                "SSEAlgorithm": "aws:kms",
                                "KMSMasterKeyID": response["KeyMetadata"]["KeyId"],
                            }
                        }
                    ]
                },
            )
        except Exception as e:
            delete_bucket(bucket_name=name, org_name=org_name)
            raise Exception("Failed to create encryption. Bucket was deleted", e)

    if https_only:
        try:
            s3_client.put_bucket_policy(
                Bucket=name,
                Policy=json.dumps(
                    {
                        "Statement": [
                            {
                                "Action": "s3:*",
                                "Effect": "Deny",
                                "Principal": "*",
                                "Resource": "arn:aws:s3:::" + name + "/*",
                                "Condition": {"Bool": {"aws:SecureTransport": False}},
                            }
                        ]
                    }
                ),
            )
        except Exception as e:
            delete_bucket(bucket_name=name, org_name=org_name)
            raise Exception("Failed to create http enforcement. Bucket was deleted", e)

    logger.info(f"Created S3 bucket {name} in org {org_name} ")


def is_aggregated(query):
    # aggregated_tokens = {"AVG","SUM", "GROUPBY"}
    # res  = sqlparse.parse(query)
    # stmt = res[0]
    #
    # tokens = set([str(t) for t in stmt.tokens])
    #
    # if aggregated_tokens.intersection(tokens):
    #     return True #TODO as for now no diffrential privacy. considering any query as aggregated even if not.
    # return False

    return True


class MyTokenAuthentication(TokenAuthentication):
    keyword = "Bearer"

    def authenticate_credentials(self, key):
        model = self.get_model()
        try:
            token = model.objects.select_related("user").get(key=key)
        except model.DoesNotExist:
            raise AuthenticationFailed("Invalid token.")

        if not token.user.is_active:
            raise AuthenticationFailed("User inactive or deleted")

        # This is required for the time comparison
        now = dt.now()

        if token.created < now - td(hours=settings.token_valid_hours):
            raise AuthenticationFailed("Token has expired")

        # if there are issues with multiple tokens for users uncomment
        # token.created = now
        # token.save()

        return token.user, token


@with_glue_client
def create_glue_database(boto3_client, org_name, dataset):
    logger.info(
        f"Creating glue database {dataset.glue_database} for dataset {dataset.name}:{dataset.id} "
        f"in org {org_name} "
    )
    boto3_client.create_database(DatabaseInput={"Name": dataset.glue_database})

    dataset.save()


# This function should be running inside a thread! thus it's swallow errors
def process_cohort_users(org_name, data_source, columns, data_filter, orig_data_source):
    logger.info(
        f"processing cohort users for data_source {data_source.name}:{data_source.id} "
        f"in org {org_name} "
    )
    data_source.set_as_pending()
    try:
        for dataset_user in data_source.dataset.datasetuser_set.all():
            logger.info(
                f"processing data for user {dataset_user.user.id} with permission {dataset_user.permission} "
                f"in data_source {data_source.name}:{data_source.id} "
                f"in org {org_name} "
            )
            if dataset_user.permission == "limited_access":
                limited = dataset_user.permission_key
                logger.info(
                    f"creating limited table for user {dataset_user.user.id} with limited {limited} "
                )
                query, _ = devexpress_filtering.dev_express_to_sql(
                    table=f"{orig_data_source.dir}_limited_{limited}",
                    schema=orig_data_source.dataset.glue_database,
                    data_filter=data_filter,
                    columns=columns,
                )
                create_limited_glue_table(
                    data_source=data_source,
                    org_name=org_name,
                    limited=limited,
                    query=query,
                )
        data_source.set_as_ready()
    except Exception as e:
        logger.exception(
            f"Failed processing user in the data source {data_source.name} ({data_source.id}) with error {e}"
        )
        data_source.set_as_error()


@with_glue_client
def process_datasource_glue_and_bucket_data(boto3_client, org_name, data_source):
    try:
        # move the file that was uploaded by front-end to another place (_full)
        update_folder_hierarchy(data_source=data_source, org_name=org_name)

        # create agg stat and limited (only if dataset default permission is limited)
        create_agg_stats(data_source=data_source, org_name=org_name)

        # create limited table if default dataset permission is limited
        limited = data_source.permission_key
        if limited:
            create_limited_glue_table(
                data_source=data_source, org_name=org_name, limited=limited
            )

        # create limited for limited users for dataset
        for dataset_user in data_source.dataset.limited_dataset_users:
            create_limited_glue_table(
                data_source=data_source,
                org_name=org_name,
                limited=dataset_user.permission_key,
            )

        # process all connected studies into the data_source's dataset.
        # create limited versions according to permission_attributes in the studies
        for study_dataset in data_source.dataset.studydataset_set.all():
            study_dataset.process()

        # connect glue to the updated _full file.
        update_glue_table(data_source=data_source, org_name=org_name)

        data_source.set_as_ready()
        logger.info(
            f"Done processing data_source {data_source.name} ({data_source.id}) "
            f"in org {org_name} "
        )
    except Exception as e:
        logger.exception(
            f"Failed uploading the data source {data_source.name} ({data_source.id}) with error {e}"
        )
        data_source.set_as_error()


@with_glue_client
def create_glue_table(boto3_client, org_name, data_source):
    """
    wait for new data-source uploaded by front-end to be crawled by glue and then process it.
    """
    create_glue_crawler(data_source=data_source, org_name=org_name)

    crawler_name = f"data_source-{data_source.id}"

    boto3_client.start_crawler(Name=crawler_name)
    logger.info(
        f"Glue database crawler for datasource {data_source.name}:{data_source.id} "
        f"in dataset {data_source.dataset.name}:{data_source.dataset.id} for org_name {org_name} "
        f"was created and started successfully"
    )
    crawler_ready = False
    max_retries = 500
    retries = max_retries

    while not crawler_ready and retries >= 0:
        res = boto3_client.get_crawler(Name=crawler_name)
        crawler_ready = True if res["Crawler"]["State"] == "READY" else False
        sleep(5)
        retries -= 1

    logger.info(
        f"Is crawler for datasource {data_source.name}:{data_source.id} "
        f"in dataset {data_source.dataset.name}:{data_source.dataset.id} "
        f"for org_name {org_name} has finished: {crawler_ready}"
    )
    boto3_client.delete_crawler(Name="data_source-" + str(data_source.id))
    if not crawler_ready:
        logger.warning(
            f"The crawler for data_source {data_source.name}:{data_source.id} in org {org_name} "
            f"had a failure after {max_retries} tries"
        )
        data_source.set_as_error()

    else:
        logger.debug(
            f"The crawler for datasource {data_source.name} ({data_source.id}) in org {org_name} "
            f"was finished successfully. "
            f"Updating data_source state accordingly."
        )

        process_datasource_glue_and_bucket_data(
            org_name=org_name, data_source=data_source
        )


def process_structured_data_source_in_background(org_name, data_source):
    """
    called when data-source was uploaded.
    it will run a thread to process all of the data-source files in the bucket,
    create limited/de-id/etc..
    (if needed - depending on the dataset default permission, and related study(ies) to this dataset)
    """
    data_source.set_as_pending()

    create_glue_table_thread = threading.Thread(
        target=create_glue_table,
        kwargs={"org_name": org_name, "data_source": data_source},
    )  # also setting the data_source state to ready when it's done
    create_glue_table_thread.start()


# This function should be running inside a thread!
# It will call process_cohort_users which swallow errors
def create_glue_tables_for_cohort(
    org_name, data_source, columns, data_filter, orig_data_source
):
    process_datasource_glue_and_bucket_data(org_name=org_name, data_source=data_source)
    process_cohort_users(
        data_source=data_source,
        org_name=org_name,
        columns=columns,
        data_filter=data_filter,
        orig_data_source=orig_data_source,
    )


def process_structured_cohort_in_background(
    org_name, data_source, columns, data_filter, orig_data_source
):
    """
    process data_source glue tables
    and create limited glue tables for limited users
    each datasource will call create_glue_tables_for_cohort function will run inside a thread!
    create_glue_tables_for_cohort will call process_cohort_users which swallow errors
    """
    data_source.set_as_pending()

    create_glue_table_thread = threading.Thread(
        target=create_glue_tables_for_cohort,
        kwargs={
            "org_name": org_name,
            "data_source": data_source,
            "columns": columns,
            "data_filter": data_filter,
            "orig_data_source": orig_data_source,
        },
    )  # also setting the data_source state to ready when it's done
    create_glue_table_thread.start()


def create_limited_table_for_all_dataset_data_sources_in_threads(
    dataset, limited_value
):
    organization_name = dataset.organization.name

    def thread(ds):
        ds.set_as_pending()
        try:
            create_limited_glue_table(
                data_source=ds, org_name=organization_name, limited=limited_value
            )
            ds.set_as_ready()
        except Exception as e:
            logger.exception(e)
            ds.set_as_error()

    executor.map(thread, dataset.data_sources.all())


def process_structured_data_sources_in_background(dataset):
    if dataset.default_user_permission == "limited_access":
        create_limited_table_for_all_dataset_data_sources_in_threads(
            dataset=dataset, limited_value=dataset.permission_key
        )


@with_s3_resource
def determine_data_source_s3_object_from_execution_id(
    boto3_client, query_execution_id, org_name, dataset
):
    bucket = dataset.bucket
    key = f"temp_execution_results/tables/{query_execution_id}-manifest.csv"
    obj = boto3_client.Object(bucket, key)
    size = obj.content_length
    body = obj.get()["Body"].read().decode("utf-8")
    path, file_name, file_name_no_ext, ext = break_s3_object(body.strip("\n"))
    path = path.split("/")[-1]

    key = f"{path}/{file_name}"

    return {"key": key, "size": size}


@with_glue_client
def update_glue_table(boto3_client, data_source, org_name):
    # get data from current table
    try:
        response = boto3_client.get_table(
            DatabaseName=data_source.dataset.glue_database, Name=data_source.glue_table
        )
    except Exception as e:
        return ErrorResponse("Error fetching current glue table", e)

    new_table_name = f"{data_source.dir}_full"
    table_input = response["Table"]
    table_input["Name"] = new_table_name

    table_input.pop("CreatedBy")
    table_input.pop("CreateTime")
    table_input.pop("UpdateTime")
    table_input.pop("IsRegisteredWithLakeFormation")
    table_input.pop("DatabaseName")
    table_input["StorageDescriptor"][
        "Location"
    ] = f"s3://lynx-dataset-{data_source.dataset.id}/{data_source.dir}/{LYNX_STORAGE_DIR}/{PrivilegePath.FULL.value}/"

    try:
        boto3_client.create_table(
            DatabaseName=data_source.dataset.glue_database, TableInput=table_input
        )

        boto3_client.delete_table(
            DatabaseName=data_source.dataset.glue_database, Name=data_source.glue_table
        )

    except Exception as e:
        return ErrorResponse("Error migrating glue table", e)

    data_source.glue_table = new_table_name
    data_source.save()


@with_s3_resource
def update_folder_hierarchy(boto3_client, data_source, org_name):
    s3_bucket = data_source.bucket
    s3_client = boto3_client.meta.client

    # create folders
    data_source_dir = data_source.dir
    base_dir = f"{data_source_dir}/{LYNX_STORAGE_DIR}"
    full_access_dir = f"{base_dir}/{PrivilegePath.FULL.value}/"
    agg_stat_dir = f"{base_dir}/{PrivilegePath.AGG_STATS.value}/"

    # create folders
    s3_client.put_object(Bucket=s3_bucket, Key=full_access_dir, ACL="private")
    s3_client.put_object(Bucket=s3_bucket, Key=agg_stat_dir, ACL="private")

    s3_objects_all = data_source.s3_objects

    for index, s3_object in enumerate(s3_objects_all):
        s3_object_key = s3_object["key"]
        file_name = s3_object_key.split("/")[-1]
        new_key = f"{data_source_dir}/{LYNX_STORAGE_DIR}/{PrivilegePath.FULL.value}/{file_name}"
        try:
            copy_source = f"{s3_bucket}/{s3_object_key}"
            boto3_client.Object(s3_bucket, new_key).copy_from(CopySource=copy_source)
        except botocore.exceptions.ClientError as e:
            return ErrorResponse(f"Unable to Move file with key {s3_object_key}!", e)
        data_source.s3_objects[index]["key"] = new_key
        data_source.save()
        try:
            boto3_client.Object(s3_bucket, s3_object_key).delete()
        except botocore.exceptions.ClientError as e:
            logger.warning(f"Unable to delete file with key {s3_object_key}!")

    logger.info(
        f"Updated folder hierarchy for datasource {data_source.name}:{data_source.id} in org {org_name}"
    )


@with_athena_client
def create_limited_glue_table(boto3_client, data_source, org_name, limited, query=None):
    logger.info(
        f"creating limited table for data_source {data_source.id} limited={limited}"
    )

    dataset = data_source.dataset
    destination_glue_database = dataset.glue_database
    destination_glue_table = data_source.glue_table
    bucket = dataset.bucket
    destination_dir = f"{bucket}/{data_source.dir}/{LYNX_STORAGE_DIR}/{PrivilegePath.LIMITED.value}_{limited}/"
    if not query:
        # noinspection SqlNoDataSourceInspection
        query = (
            f'SELECT * FROM "{destination_glue_database}"."{destination_glue_table}" '
            f"ORDER BY RANDOM() limit {limited};"
        )
    # noinspection SqlNoDataSourceInspection
    ctas_query = (
        f'CREATE TABLE "{destination_glue_database}"."{data_source.dir}_limited_{limited}" '
        f"WITH (format = 'TEXTFILE', external_location = 's3://{destination_dir}') "
        f"AS {query};"
    )

    logger.debug(f"Query result of CREATE TABLE AS SELECT {ctas_query}")

    try:
        query_results = boto3_client.start_query_execution(
            QueryString=ctas_query,
            QueryExecutionContext={"Database": destination_glue_database},
            ResultConfiguration={
                "OutputLocation": f"s3://{bucket}/temp_execution_results"
            },
        )

        logger.info(
            f"limited file created for datasource {data_source.id} limited {limited} at {destination_dir} in bucket {bucket}"
        )

        return query_results
    except boto3_client.exceptions.InvalidRequestException as e:
        error = Exception(
            f"Failed executing the CTAS query: {ctas_query}. "
            f"Query string: {ctas_query}"
        ).with_traceback(e.__traceback__)
        logger.debug(f"This is the ctas_query {ctas_query}")
        raise error from e


@with_s3_client
def create_agg_stats(boto3_client, data_source, org_name):
    # get statistics
    stats, _ = calculate_statistics(data_source)

    # create CSV
    columns = stats["result"][0].keys()
    temp_dir = tempfile.TemporaryDirectory(str(data_source.id))
    temp_file_name = os.path.join(temp_dir.name, data_source.name)
    with open(temp_file_name, "w") as stats_file:
        dict_writer = csv.DictWriter(stats_file, columns)
        dict_writer.writeheader()
        dict_writer.writerows(stats["result"])

    # upload to S3
    try:
        boto3_client.upload_file(
            Bucket=data_source.bucket,
            Key=f"{data_source.dir}/{LYNX_STORAGE_DIR}/{PrivilegePath.AGG_STATS.value}/{data_source.name}",
            Filename=temp_file_name,
        )
    except BucketNotFound as e:
        raise BucketNotFound(
            f"The bucket for uploading agg stat datasource does not exist. "
            f"Bucket: {data_source.bucket}, in org {org_name}"
        ) from e
    except boto3_client.exceptions.ClientError as e:
        raise ForbiddenErrorResponse(
            f"Missing permissions to upload file to bucket: {data_source.bucket}, in org {org_name}",
            e,
        )
    except Exception as e:
        return ErrorResponse(
            f"There was an error uploading the agg stat file for datasource {data_source.name}",
            error=e,
        )
    finally:
        temp_dir.cleanup()

    logger.info(f"Created AggStats for datasource {data_source} in org {org_name}")


def calculate_statistics(data_source, query_from_front=None):
    dataset = data_source.dataset
    org_name = dataset.organization.name
    glue_database = dataset.glue_database
    glue_table = data_source.glue_table
    bucket_name = data_source.bucket

    try:
        columns_types = get_columns_types(
            org_name=org_name, glue_database=glue_database, glue_table=glue_table
        )
        default_athena_col_names = statistics.create_default_column_names(columns_types)
    except UnableToGetGlueColumns as e:
        return ErrorResponse(f"Glue error", error=e)
    try:
        filter_query = (
            None
            if not query_from_front
            else devexpress_filtering.generate_where_sql_query(query_from_front)
        )
        query = statistics.sql_builder_by_columns_types(
            glue_table, columns_types, default_athena_col_names, filter_query
        )
    except UnsupportedColumnTypeError as e:
        return UnimplementedErrorResponse("There was some error in execution", error=e)
    except Exception as e:
        return ErrorResponse("There was some error in execution", error=e)

    try:
        response = statistics.count_all_values_query(
            query, glue_database, bucket_name, org_name
        )
        data_per_column = statistics.sql_response_processing(
            response, default_athena_col_names
        )
        final_result = {"result": data_per_column, "columns_types": columns_types}
    except QueryExecutionError as e:
        return ErrorResponse(
            "There was some error in execution", error=e, status_code=502
        )
    except (InvalidExecutionId, MaxExecutionReactedError) as e:
        return ErrorResponse("There was some error in execution", error=e)
    except KeyError as e:
        return ErrorResponse(
            "Unexpected error: invalid or missing query result set", error=e
        )
    except Exception as e:
        return ErrorResponse("There was some error in execution", error=e)

    return final_result, response


@organization_dependent
def create_glue_crawler(org_settings, data_source, org_name):
    logger.info(
        f"Started create_glue_crawler for datasource {data_source.name}:{data_source.id} "
        f"in dataset {data_source.dataset.name}:{data_source.dataset.id} in org {org_name}"
    )
    glue_client = aws_service.create_glue_client(org_name=org_name)

    path, file_name, file_name_no_ext, ext = break_s3_object(
        data_source.s3_objects[0]["key"]
    )
    glue_client.create_crawler(
        Name="data_source-" + str(data_source.id),
        Role=org_settings["AWS_GLUE_SERVICE_ROLE"],
        DatabaseName="dataset-" + str(data_source.dataset.id),
        Description="",
        Targets={
            "S3Targets": [
                {"Path": f"s3://{data_source.dataset.bucket}/{path}/", "Exclusions": []}
            ]
        },
        SchemaChangePolicy={
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DELETE_FROM_DATABASE",
        },
    )


@with_s3_client
def handle_zipped_data_source(boto3_client, data_source, org_name):
    s3_obj = data_source.s3_objects[0]["key"]
    path, file_name, file_name_no_ext, ext = break_s3_object(s3_obj)

    workdir = "/tmp/" + str(data_source.id) + "/" + file_name_no_ext
    os.makedirs(workdir + "/extracted")
    boto3_client.download_file(
        data_source.dataset.bucket, s3_obj, workdir + "/" + file_name
    )
    zip_ref = zipfile.ZipFile(workdir + "/" + file_name, "r")
    try:
        zip_ref.extractall(workdir + "/extracted")
    except:
        data_source.state = "error: failed to extract zip file"
    zip_ref.close()
    subprocess.check_output(
        [
            "aws",
            "s3",
            "sync",
            f"{workdir}/extracted",
            f"s3://{data_source.dataset.bucket}/{path}/{file_name_no_ext}",
        ]
    )
    shutil.rmtree(f"/tmp/{str(data_source.id)}")
    data_source.set_as_ready()


def calc_access_to_database(user, dataset):
    if dataset.state == "private":
        if user.permission(dataset) == "aggregated_access":
            return "aggregated access"
        elif user.permission(dataset) in ["admin", "full_access"]:
            return "full access"
        else:  # user not aggregated and not full or admin
            if dataset.default_user_permission == "aggregated_access":
                return "aggregated access"
            elif dataset.default_user_permission == "limited_access":
                return "limited access"
            elif dataset.default_user_permission == "no access":
                return "no access"
    elif dataset.state == "public":
        return "full access"

    return "no permission"  # safe. includes archived dataset


def load_tags(delete_removed_tags=True):
    with open("tags.json") as f:
        tags = json.load(f)
        if delete_removed_tags:
            models.Tag.objects.all().delete()

        for tag in tags:
            tag_db, created = models.Tag.objects.get_or_create(
                name=tag["tag_name"], category=tag["category"]
            )
            if not created:
                logger.warning(
                    f"Warning, duplicated tag was not created in load_tags. Already exist in the database: {tag}"
                )


@with_s3_client
def get_s3_object(boto3_client, bucket, key, org_name, s3_client=None, retries=60):
    if not s3_client:
        s3_client = boto3_client

    while True:
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            logger.exception(
                f"Object {key} was fetched successfully from s3 bucket {bucket} in org {org_name}"
            )
            return obj
        except s3_client.exceptions.NoSuchKey:
            if retries > 0:
                sleep(1)
                retries -= 1

                continue
            logger.exception(
                f"Failed to fetch object {key} from s3 bucket {bucket} in org {org_name}"
            )
            raise
        except s3_client.exceptions.NoSuchBucket as e:
            raise BucketNotFound(bucket) from e


def csv_to_json(csv, columns_types):
    def convert(value, type):

        if value in ("", '""'):
            return None
        try:
            if type == "bigint":
                return int(value)
            if type == "double":
                return float(value)
        except:
            return str(value)

        return str(value)

    dic = {}

    rows = csv.split("\n")
    columns_name = rows[0].split(",")
    for i, column_name in enumerate(columns_name):
        dic[column_name] = []

        for row in rows[1:]:
            cols = row.split(",")
            dic[column_name].append(convert(cols[i], columns_types[i]["Type"]))

    return dic


@with_glue_client
def get_columns_types(boto3_client, org_name, glue_database, glue_table):
    try:
        response = boto3_client.get_table(DatabaseName=glue_database, Name=glue_table)
    except boto3_client.exceptions.EntityNotFoundException as e:
        raise UnableToGetGlueColumns from e

    columns_types = response["Table"]["StorageDescriptor"]["Columns"]
    return columns_types


def get_query_no_limit_and_count_query(query):
    res = sqlparse.parse(query)
    stmt = res[0]

    tokens_values = [x.value.lower() for x in stmt.tokens]

    limit = None
    if "limit" in tokens_values:
        i_limit = tokens_values.index("limit")
        limit = int(str(stmt.tokens[i_limit + 2]))
        del stmt.tokens[i_limit : i_limit + 3]

    where_clauses = stmt[8].value if len(list(stmt)) > 8 else ""

    count_query = "SELECT COUNT(*) FROM " + stmt[6].value + " " + where_clauses
    query_no_limit = str(stmt)

    return query_no_limit, count_query, limit


@with_s3_client
def list_objects_version(
    boto3_client,
    bucket,
    org_name,
    filter=None,
    exclude=None,
    start=None,
    end=None,
    prefix="",
):
    import fnmatch
    import pytz

    items = boto3_client.list_object_versions(Bucket=bucket, Prefix=prefix)["Versions"]

    if start and end:
        assert start <= end, "start is has to be before end"

    if filter:
        items = [x for x in items if fnmatch.fnmatch(x["Key"], filter)]

    if exclude:
        items = [x for x in items if not fnmatch.fnmatch(x["Key"], exclude)]

    if start:
        if not start.tzinfo:
            start = start.replace(tzinfo=pytz.utc)
        items = [x for x in items if x["LastModified"] >= start]

    if end:
        if not end.tzinfo:
            end = end.replace(tzinfo=pytz.utc)
        items = [x for x in items if x["LastModified"] <= end]

    return items


@with_s3_resource
def delete_bucket(boto3_client, bucket_name, org_name):
    try:
        bucket = boto3_client.Bucket(bucket_name)
        bucket.object_versions.delete()
        bucket.objects.all().delete()
        bucket.delete()
        logger.info(f"Deleted bucket: {bucket_name} in org {org_name}")
    except boto3_client.meta.client.exceptions.NoSuchBucket as e:
        raise BucketNotFound(bucket_name) from e


@with_iam_resource
def delete_role_and_policy(boto3_client, bucket_name, org_name):
    role = boto3_client.Role(bucket_name)
    policy_arn = role.arn.replace("role", "policy")
    policy = boto3_client.Policy(policy_arn)

    try:
        role.detach_policy(PolicyArn=policy_arn)
        role.delete()
    except boto3_client.exceptions.NoSuchEntityException as e:
        raise RoleNotFound(role) from e

    try:
        policy.delete()
    except boto3_client.exceptions.NoSuchEntityException as e:
        raise PolicyNotFound(policy_arn) from e


def set_policy_clear_athena_history(
    s3_bucket, s3_client, expiration=1, prefix="temp_execution_results/"
):
    return s3_client.put_bucket_lifecycle_configuration(
        Bucket=s3_bucket,
        LifecycleConfiguration={
            "Rules": [
                {
                    "Expiration": {"Days": expiration},
                    "Filter": {"Prefix": prefix},
                    "Status": "Enabled",
                }
            ]
        },
    )


def get_client_ip(request):
    client_address = request.META.get("HTTP_X_FORWARDED_FOR")
    return (
        client_address.split(",")[0]
        if client_address
        else request.META.get("REMOTE_ADDR")
    )
