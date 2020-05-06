import json
import os
import shutil
import subprocess
import zipfile
from datetime import datetime as dt, timedelta as td
from time import sleep

import logging

import botocore
import pytz
import requests
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
)
from mainapp.utils import aws_service
from mainapp.utils.decorators import (
    organization_dependent,
    with_glue_client,
    with_s3_client,
    with_s3_resource,
    with_iam_resource,
)
from mainapp.utils.response_handler import ForbiddenErrorResponse

logger = logging.getLogger(__name__)


def break_s3_object(obj):
    file_name = obj.split("/")[-1]
    file_name_no_ext = ".".join(file_name.split(".")[:-1])
    ext = file_name.split(".")[-1]
    path = "/".join(obj.split("/")[:-1])

    return path, file_name, file_name_no_ext, ext


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
    if org_settings["AWS_REGION"] == "us-east-1":
        s3_client.create_bucket(Bucket=name)
    else:
        s3_client.create_bucket(
            Bucket=name,
            CreateBucketConfiguration={
                "LocationConstraint": org_settings["AWS_REGION"]
            },
        )

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
            f"The bucket queried does not exist. Bucket: {name}, in org {org_name}", e
        )
    except botocore.exceptions.ClientError as e:
        raise ForbiddenErrorResponse(
            f"Missing s3:PutBucketPublicAccessBlock permissions to put public access block policy for bucket: {name}, in org {org_name}",
            e,
        )
    except Exception as e:
        raise Exception(
            f"There was an error when removing public access from bucket: {name} in org {org_name}",
            e,
        )

    if encrypt:
        logger.debug(f"Creating encrypted bucket: {name} for org_name {org_name}")
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
        f"Creating glue database {dataset.glue_database} for org_name {org_name} for following dataset.id {dataset.id}"
    )
    boto3_client.create_database(DatabaseInput={"Name": dataset.glue_database})

    dataset.save()


@with_glue_client
def create_catalog(boto3_client, org_name, data_source):
    create_glue_crawler(data_source=data_source, org_name=org_name)

    boto3_client.start_crawler(Name=f"data_source-{data_source.id}")
    logger.info(
        f"Glue database crawler for dataset.id {data_source.id} for org_name {org_name}"
        f"was created and started successfully"
    )
    crawler_ready = False
    retries = 50

    while not crawler_ready and retries >= 0:
        res = boto3_client.get_crawler(Name=f"data_source-{data_source.id}")
        crawler_ready = True if res["Crawler"]["State"] == "READY" else False
        sleep(5)
        retries -= 1

    logger.info(
        f"Is crawler for datasource.id {data_source.id} for org_name {org_name} has finished: {crawler_ready}"
    )
    if not crawler_ready:
        logger.warning(
            f"The crawler for data_source {data_source.name} ({data_source.id}) in organization {org_name} "
            f"had a failure after 3 tries"
        )
        data_source.state = "crawling error"
        data_source.save()

    else:
        logger.debug(
            f"The crawler for datasource {data_source.name} ({data_source.id}) in organization {org_name} "
            f"was finished succesfully. "
            f"Updating data_source state accordingly."
        )
        boto3_client.delete_crawler(Name="data_source-" + str(data_source.id))
        data_source.state = "ready"
        data_source.save()


@organization_dependent
def create_glue_crawler(org_settings, data_source, org_name):
    logger.info(
        f"Started create_glue_crawler for datasource.id {data_source.id} for org_name {org_name}"
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
    data_source.state = "ready"
    data_source.save()


#
# def clean(string):
#     return ''.join(e for e in string.replace("-", " ").replace(" ", "c83b4ce5") if e.isalnum()).lower().replace("c83b4ce5", "-")


def calc_access_to_database(user, dataset):
    if dataset.state == "private":
        if user.permission(dataset) == "aggregated_access":
            return "aggregated access"
        elif user.permission(dataset) in ["admin", "full_access"]:
            return "full access"
        else:  # user not aggregated and not full or admin
            if dataset.default_user_permission == "aggregated_access":
                return "aggregated access"
            elif dataset.default_user_permission == "no access":
                return "no access"
    elif dataset.state == "public":
        return "full access"

    return "no permission"  # safe. includes archived dataset


def close_all_jh_running_servers(idle_for_hours=0):
    import dateparser
    from datetime import datetime as dt, timedelta as td

    aws_response = requests.get(
        "http://169.254.169.254/latest/dynamic/instance-identity/document"
    )
    aws_response_json = aws_response.json()["accountId"]
    for account_key, account_value in settings.ORG_VALUES.items():
        if account_value["ACCOUNT_NUMBER"] == aws_response_json:
            org_name = account_key
        else:
            logger.exception("No such account")
    headers = {
        "Authorization": "Bearer "
        + settings.ORG_VALUES[org_name]["JH"]["JH_API_ADMIN_TOKEN"],
        "ALBTOKEN": settings.ORG_VALUES[org_name]["JH"]["JH_ALB_TOKEN"],
    }
    res = requests.get(
        settings.ORG_VALUES[org_name]["JH"]["JH_URL"] + "hub/api/users",
        headers=headers,
        verify=False,
    )
    assert res.status_code == 200, "error getting users: " + res.text
    users = json.loads(res.text)
    for user in users:
        # if user['admin']:
        #     continue
        if user["server"]:
            last_activity = user["last_activity"] or user["created"]
            idle_time = dt.now(tz=pytz.UTC) - dateparser.parse(last_activity)
            if idle_time > td(hours=idle_for_hours):
                res = requests.delete(
                    settings.ORG_VALUES[org_name]["JH"]["JH_URL"]
                    + "hub/api/users/"
                    + user["name"]
                    + "/server",
                    headers=headers,
                    verify=False,
                )
                logger.debug(
                    f"User: {user['name']} "
                    f"idle time: {idle_time}, {str(res.status_code)}, {res.text}"
                )
            else:
                logger.debug(
                    f"User: {user['name']} "
                    f"idle time: {idle_time} < {td(hours=idle_for_hours)}"
                )


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
                f"Object {key} was fetched successfully from s3 bucket {bucket} for org_name {org_name}"
            )
            return obj
        except s3_client.exceptions.NoSuchKey:
            if retries > 0:
                sleep(1)
                retries -= 1

                continue
            logger.exception(
                f"Failed to fetch object {key} from s3 bucket {bucket} for org_name {org_name}"
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
        logger.debug("Deleted bucket: " + bucket_name)
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
