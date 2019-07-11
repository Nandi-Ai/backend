import logging
import os
from mainapp import settings
from rest_framework.authentication import TokenAuthentication
from rest_framework.exceptions import AuthenticationFailed
from datetime import datetime as dt,timedelta as td
import sqlparse
import boto3
from time import sleep


def break_s3_object(obj):
    file_name = obj
    file_name_no_ext = obj
    ext = obj
    path = obj

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
    #TODO create the catalog

    # Clients
    glue_client = boto3.client('glue')

    dataset = data_source.dataset

    if not dataset.glue_database:
        dataset.glue_database = "dataset-"+str(data_source.dataset.id)
        glue_client.create_database(
            DatabaseInput={
                "Name": dataset.glue_database
            }
        )

        create_glue_crawler(dataset) #if no dataset no crawler

    glue_client.start_crawler(Name="dataset-"+str(dataset.id))

    crawler_ready = False

    while not crawler_ready:
        res = glue_client.get_crawler(
            Name="dataset-"+str(dataset.id)
        )
        crawler_ready = True if res['Crawler']['State'] == 'READY' else False
        sleep(2)

    data_source.state = "ready"
    data_source.save()

def create_glue_crawler(dataset):
    glue_client = boto3.client('glue')

    glue_client.create_crawler(
        Name="dataset-"+str(dataset.id),
        Role='AWSGlueServiceRoleDefault',
        DatabaseName="dataset-"+str(dataset.id),
        Description='',
        Targets={
            'S3Targets': [
                {
                    'Path': 's3://' + dataset.bucket+"/structured",
                    'Exclusions': []
                },
            ]
        },
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DELETE_FROM_DATABASE'
        })

