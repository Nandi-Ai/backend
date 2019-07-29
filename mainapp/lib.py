import logging
import os
from mainapp import settings
from rest_framework.authentication import TokenAuthentication
from rest_framework.exceptions import AuthenticationFailed
from datetime import datetime as dt,timedelta as td
import sqlparse
import boto3
from time import sleep
import zipfile
import subprocess
import shutil


def break_s3_object(obj):
    file_name = obj.split("/")[-1]
    file_name_no_ext = ".".join(file_name.split(".")[:-1])
    ext = file_name.split(".")[-1]
    path = "/".join(obj.split("/")[:-1])

    return path, file_name, file_name_no_ext, ext

def startup():
    os.environ["AWS_ACCESS_KEY_ID"] = settings.aws_access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = settings.aws_secret_access_key
    os.environ["AWS_REGION"] = settings.aws_region

def validate_query(query, dataset):
    query_parsed = sqlparse(query)
    statement = query_parsed[0]

    if statement.get_type() == "SELECT":
        pass

    return True, False #validated, no reason..


def is_aggregated(query):
    query_parsed = sqlparse(query)
    statement = query_parsed[0]

    if statement.get_type() == "SELECT":
        pass

    return True #TODO as for now no diffrential privacy. considering any query as aggregated even if not.

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
        dataset.glue_database = "dataset-"+str(data_source.dataset.id)
        glue_client.create_database(
            DatabaseInput={
                "Name": dataset.glue_database
            }
        )
        dataset.save()

    print("creating database crawler")
    create_glue_crawler(data_source) #if no dataset no crawler

    print('starting the database crawler')
    glue_client.start_crawler(Name="data_source-"+str(data_source.id))

    crawler_ready = False
    retries = 50

    while not crawler_ready and retries >= 0:
        res = glue_client.get_crawler(
            Name="data_source-"+str(data_source.id)
        )
        crawler_ready = True if res['Crawler']['State'] == 'READY' else False
        sleep(5)
        retries-=1

    print("is crawler finished: ", crawler_ready)
    if not crawler_ready:
        data_source.state = "crawling error"
    else:
        data_source.state = "ready"
    data_source.save()

def create_glue_crawler(data_source):
    glue_client = boto3.client('glue', region_name=settings.aws_region)

    path, file_name, file_name_no_ext, ext = break_s3_object(data_source.s3_objects[0])
    glue_client.create_crawler(
        Name="data_source-"+str(data_source.id),
        Role='service-role/AWSGlueServiceRole-mvp',
        DatabaseName="dataset-"+str(data_source.dataset.id),
        Description='',
        Targets={
            'S3Targets': [
                {
                    'Path': 's3://' + data_source.dataset.bucket+"/"+path+"/",
                    'Exclusions': []
                },
            ]
        },
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DELETE_FROM_DATABASE'
        })


def handle_zipped_data_source(data_source):
    s3_obj = data_source.s3_objects[0]
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
        ["aws", "s3", "sync", workdir + "/extracted", "s3://" + data_source.dataset.bucket + "/" + path+"/"+file_name_no_ext])
    shutil.rmtree("/tmp/" + str(data_source.id))
    data_source.state = "ready"
    data_source.save()


def calc_permission_for_dataset(user, dataset):
    if dataset.state == "private":
        if user in dataset.aggregated_users.all():
            return "aggregated"
        elif user in dataset.full_access_users.all() or user in dataset.admin_users.all():
            return "full"
        else:  # user not aggregated and not full or admin
            if dataset.default_user_permission == "aggregated":
                return "aggregated"
            elif dataset.default_user_permission == "none":
                return "no permission"
    elif dataset.state == "public":
        return "full"

    return "no permission"  # safe. includes archived dataset
