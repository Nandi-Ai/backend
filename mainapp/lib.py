import json
import os
import shutil
import subprocess
import zipfile
from datetime import datetime as dt, timedelta as td
from time import sleep

import boto3
import pytz
import requests
from rest_framework.authentication import TokenAuthentication
from rest_framework.exceptions import AuthenticationFailed

from mainapp import settings
from mainapp.models import Tag


def break_s3_object(obj):
    file_name = obj.split("/")[-1]
    file_name_no_ext = ".".join(file_name.split(".")[:-1])
    ext = file_name.split(".")[-1]
    path = "/".join(obj.split("/")[:-1])

    return path, file_name, file_name_no_ext, ext

def create_s3_bucket(name, s3_client=None, encrypt = settings.secured_bucket, https_only = settings.secured_bucket):
    if not s3_client:
        s3_client = boto3.client('s3')
    #https://github.com/boto/boto3/issues/125
    if settings.aws_region == 'us-east-1':
        s3_client.create_bucket(Bucket=name)
    else:
        s3_client.create_bucket(Bucket=name,
                         CreateBucketConfiguration={'LocationConstraint': settings.aws_region}, )

    if encrypt:
        s3_client.put_bucket_encryption(
            Bucket=name,
            ServerSideEncryptionConfiguration={
                'Rules': [
                    {
                        'ApplyServerSideEncryptionByDefault': {
                            'SSEAlgorithm': 'aws:kms',
                            'KMSMasterKeyID': settings.aws_kms_key_id
                        }
                    },
                ]
            }
        )

    if https_only:
        s3_client.put_bucket_policy(
            Bucket=name,
            Policy = json.dumps({
                "Statement":[
                    {
                        "Action": "s3:*",
                        "Effect": "Deny",
                        "Principal": "*",
                        "Resource": "arn:aws:s3:::"+name+"/*",
                        "Condition": {
                            "Bool":
                            {"aws:SecureTransport": False}
                        }
                    }
                ]
            })
        )

def startup():
    os.environ["AWS_ACCESS_KEY_ID"] = settings.aws_access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = settings.aws_secret_access_key
    os.environ["AWS_REGION"] = settings.aws_region


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
            token = model.objects.select_related('user').get(key=key)
        except model.DoesNotExist:
            raise AuthenticationFailed('Invalid token.')

        if not token.user.is_active:
            raise AuthenticationFailed('User inactive or deleted')

        # This is required for the time comparison
        now = dt.now()

        if token.created < now - td(hours=settings.token_valid_hours):
            raise AuthenticationFailed('Token has expired')

        # if there are issues with multiple tokens for users uncomment
        # token.created = now
        # token.save()

        return token.user, token


def create_catalog(data_source):
    # Clients
    glue_client = boto3.client('glue', region_name=settings.aws_region)
    dataset = data_source.dataset

    if not dataset.glue_database:
        print("creating glue database")
        dataset.glue_database = "dataset-" + str(data_source.dataset.id)
        glue_client.create_database(
            DatabaseInput={
                "Name": dataset.glue_database
            }
        )
        dataset.save()

    print("creating database crawler")
    create_glue_crawler(data_source)  # if no dataset no crawler

    print('starting the database crawler')
    glue_client.start_crawler(Name="data_source-" + str(data_source.id))

    crawler_ready = False
    retries = 50

    while not crawler_ready and retries >= 0:
        res = glue_client.get_crawler(
            Name="data_source-" + str(data_source.id)
        )
        crawler_ready = True if res['Crawler']['State'] == 'READY' else False
        sleep(5)
        retries -= 1


    print("is crawler finished: ", crawler_ready)
    if not crawler_ready:
        data_source.state = "crawling error"
        data_source.save()

    else:
        glue_client.delete_crawler(
            Name="data_source-" + str(data_source.id)
        )
        data_source.state = "ready"
        data_source.save()


def create_glue_crawler(data_source):
    glue_client = boto3.client('glue', region_name=settings.aws_region)

    path, file_name, file_name_no_ext, ext = break_s3_object(data_source.s3_objects[0]['key'])
    glue_client.create_crawler(
        Name="data_source-" + str(data_source.id),
        Role=settings.aws_glue_service_role,
        DatabaseName="dataset-" + str(data_source.dataset.id),
        Description='',
        Targets={
            'S3Targets': [
                {
                    'Path': 's3://' + data_source.dataset.bucket + "/" + path + "/",
                    'Exclusions': []
                },
            ]
        },
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DELETE_FROM_DATABASE'
        })


def handle_zipped_data_source(data_source):
    s3_obj = data_source.s3_objects[0]['key']
    path, file_name, file_name_no_ext, ext = break_s3_object(s3_obj)

    s3_client = boto3.client('s3')
    workdir = "/tmp/" + str(data_source.id) + "/" + file_name_no_ext
    os.makedirs(workdir + "/extracted")
    s3_client.download_file(data_source.dataset.bucket, s3_obj, workdir + "/" + file_name)
    zip_ref = zipfile.ZipFile(workdir + "/" + file_name, 'r')
    try:
        zip_ref.extractall(workdir + "/extracted")
    except:
        data_source.state = "error: failed to extract zip file"
    zip_ref.close()
    subprocess.check_output(
        ["aws", "s3", "sync", workdir + "/extracted",
         "s3://" + data_source.dataset.bucket + "/" + path + "/" + file_name_no_ext])
    shutil.rmtree("/tmp/" + str(data_source.id))
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
    headers = {"Authorization": "Bearer " + settings.jh_api_admin_token,"ALBTOKEN":settings.jh_alb_token}
    res = requests.get(settings.jh_url + 'hub/api/users', headers=headers,verify=False)
    assert res.status_code == 200, "error getting users: " + res.text
    users = json.loads(res.text)

    for user in users:
        # if user['admin']:
        #     continue
        if user['server']:
            last_activity = user['last_activity'] or user['created']

            idle_time = dt.now(tz=pytz.UTC) - dateparser.parse(last_activity)
            # print(user)
            if idle_time > td(hours=idle_for_hours):
                res = requests.delete(settings.jh_url + 'hub/api/users/' + user['name'] + '/server', headers=headers,verify=False)
                print("user", user['name'], "idle time:", idle_time, str(res.status_code), res.text)
            else:
                print(user['name'], "idle time:", idle_time, "<", td(hours=idle_for_hours))


def load_tags(delete_removed_tags=True):
    with open("tags.json") as f:
        tags=json.load(f)
        if delete_removed_tags:
            Tag.objects.all().delete()

        for tag in tags:
            tagdb,created=Tag.objects.get_or_create(name = tag['tag_name'], category=tag['category'])
            if not created:
                print("duplicate:", tag)


def create_where_section(field, operator, value):
    # todo not working
    if operator == 'contains':
        # return "regexp_like({}, '(?i){}');".format(field, value)
        return "{} LIKE '%{}%'".format(field, value)

    # todo not working
    if operator == 'notcontains':
        return "{} ILIKE '%{}%'".format(field, value)

    # todo not working
    if operator == 'startswith':
        return "{} LIKE '{}%'".format(field, value)

    # todo not working
    if operator == 'endswith':
        return "{} LIKE '%{}'".format(field, value)

    if operator == "=":
        # if value is "":
        #     return "{} = ' '".format(field, value)
        if value is None:
            return "{} is null".format(field, value)
        return "{} = {}".format(field, value)

    if operator == "<>":
        # if value is "":
        #     return "{} <> ' '".format(field, value)
        if value is None:
            return "{} is not null".format(field, value)
        return "{} <> {}".format(field, value)

    if operator == ">":
        return "{} > {}".format(field, value)

    if operator == "<":
        return "{} <= {}".format(field, value)

    if operator == ">=":
        return "{} > {}".format(field, value)

    if operator == "<=":
        return "{} <= {}".format(field, value)

    # = null or = "" (is blank)
    # <> null and <> "" (is not blank)

    raise Exception("unknown operator")


def create_where_section_from_array(data_filter):
    field = data_filter[0]
    operator = data_filter[1]
    value = data_filter[2]
    return create_where_section(field, operator, value)


def dev_express_to_sql(table, data_filter, columns):
    select = ",".join(columns) if columns else "*"
    query = "SELECT %s FROM %s" % (select, table)
    if data_filter:
        query += " WHERE "

        if not isinstance(data_filter, list):
            raise Exception("invalid data filters")

        # only one filter
        if isinstance(data_filter[0], str) and isinstance(data_filter[0], str):
            query += create_where_section_from_array(data_filter)

        # in case there is one filter if "is none of" - [ "!",  [[x, =, x], "and", [x, =, x] ]
        # if isinstance(data_filter[0], str) and isinstance(data_filter[0], list):

        # multiple filters
        if isinstance(data_filter[0], list) and isinstance(data_filter[1], str):
            for data in data_filter:
                if isinstance(data, list):
                    query += create_where_section_from_array(data)
                elif isinstance(data, str):
                    query += " " + data + " "

    return query

def get_s3_object(bucket,key,s3_client=None,retries=30):
    if not s3_client:
        s3_client=boto3.client('s3')

    while True:
        try:
            obj = s3_client.get_object(Bucket=bucket,
                                       Key=key)
            return obj
        except s3_client.exceptions.NoSuchKey:
            if retries>0:
                sleep(0.5)
                retries-=1

                continue
            raise

def csv_to_json(csv,columns_types):
    def convert(value, type):
        if value in ('', '""'):
            return None
        elif type == 'bigint':
            return int(value)
        elif type == 'double':
            return float(value)
        else:
            return str(value)

    dic={}

    rows = csv.split('\n')
    columns_name = rows[0].split(',')
    for i, column_name in enumerate(columns_name):
        dic[column_name] = []

        for row in rows[1:]:
            cols = row.split(",")
            dic[column_name].append(convert(cols[i], columns_types[i]['Type']))

    return dic












