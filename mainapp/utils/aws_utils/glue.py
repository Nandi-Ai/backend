from mainapp.utils.decorators import with_glue_client


@with_glue_client
def delete_database(boto3_client, glue_database, org_name):
    boto3_client.delete_database(Name=glue_database)
