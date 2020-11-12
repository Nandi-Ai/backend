from mainapp.utils.decorators import with_s3_resource


def download_file(s3_client, bucket_name, s3_object, file_path):
    return s3_client.download_file(bucket_name, s3_object, file_path)


def upload_file(s3_client, csv_path_and_file, bucket_name, file_path):
    return s3_client.upload_file(csv_path_and_file, bucket_name, file_path)


@with_s3_resource
def delete_directory(boto3_client, bucket_name, directory, org_name):
    bucket = boto3_client.Bucket(bucket_name)
    bucket.objects.filter(Prefix=f"{directory}/").delete()


TEMP_EXECUTION_DIR = "temp_execution_results"
