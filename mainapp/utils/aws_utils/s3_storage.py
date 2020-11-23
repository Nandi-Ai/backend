from mainapp.utils.decorators import with_s3_resource


def download_file(s3_client, bucket_name, s3_object, file_path):
    return s3_client.download_file(bucket_name, s3_object, file_path)


def upload_file(s3_client, csv_path_and_file, bucket_name, file_path):
    return s3_client.upload_file(csv_path_and_file, bucket_name, file_path)


@with_s3_resource
def delete_directory(boto3_client, bucket_name, directory, org_name):
    bucket = boto3_client.Bucket(bucket_name)
    bucket.objects.filter(Prefix=f"{directory}/").delete()


@with_s3_resource
def list_objects(boto3_client, bucket_name, prefix):
    next_token = str()
    list_objects_args = {"Bucket": bucket_name, "Prefix": prefix}
    while next_token is not None:
        if next_token:
            list_objects_args.update({"ContinuationToken": next_token})

        objects_in_bucket = boto3_client.list_objects_v2(**list_objects_args)
        for obj in objects_in_bucket["Contents"]:
            yield obj

        next_token = objects_in_bucket.get("NextContinuationToken")


TEMP_EXECUTION_DIR = "temp_execution_results"
