def download_file(s3_client, bucket_name, s3_object, file_path):
    return s3_client.download_file(bucket_name, s3_object, file_path)


def upload_file(s3_client, csv_path_and_file, bucket_name, file_path):
    return s3_client.upload_file(csv_path_and_file, bucket_name, file_path)
