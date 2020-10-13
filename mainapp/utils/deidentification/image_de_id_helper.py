import json
import logging
import re

from datetime import datetime
from io import BytesIO

from botocore.exceptions import ClientError
from mainapp.utils.lib import break_s3_object
from mainapp.utils import aws_service
from mainapp.utils.deidentification.common.image_de_id_exceptions import (
    EmptyBucketError,
    UpdateJobProcessError,
    BaseImageDeIdHelperError,
    NoObjectContentError,
)

logger = logging.getLogger(__name__)


class ImageDeIdHelper(object):
    __METHOD_TIME_THRESHOLD = 40

    def __init__(self, dataset):
        self.__dataset = dataset
        self.__s3_resource = aws_service.create_s3_resource(
            org_name=dataset.organization.name
        )
        self.__s3_client = self.__s3_resource.meta.client
        self.__source_bucket = dataset.bucket

    def update_images_method_status(self):
        for method in self.__dataset.methods.all():
            if method.state == "pending":
                for dsrc_method in method.data_source_methods.filter(state="pending"):
                    if dsrc_method.data_source.type == "images":
                        try:
                            self.__update_dsrc_method_status(dsrc_method, method)
                        except BaseImageDeIdHelperError as e:
                            dsrc_method.set_as_error()

                            logger.warning(
                                f"Could not update method {method.id} and dsrc {dsrc_method.id}",
                                e,
                            )

    def __update_dsrc_method_status(self, dsrc_method, method):
        job_id_file_name = f"{dsrc_method.data_source.id}-image-job-processing.json"
        destination_location = f"{dsrc_method.data_source.dir}/lynx-storage/status_deid_access_{method.id}_json"
        bucket_content = self.__list_bucket_objects(destination_location)

        successful_processed_images = self.__get_successful_images(bucket_content)

        if successful_processed_images == len(dsrc_method.data_source.s3_objects):
            job_status = "Completed"
            dsrc_method.set_as_ready()
            logger.info(
                f"Image De-id Method {dsrc_method.data_source.id} ready. Please look for job_processed_status"
                f"{job_id_file_name}"
            )
        else:
            job_status = self.__validate_status(method, dsrc_method, job_id_file_name)

        self.__create_json_job_process_content(
            destination_location=destination_location,
            job_id_file_name=job_id_file_name,
            job_processed_status=job_status,
            processed_images_list=bucket_content,
            successful_processed_images=successful_processed_images,
        )

    def __validate_status(self, method, dsrc_method, job_id_file_name):
        method_time_creation = datetime.strptime(
            str(method.created_at.replace(tzinfo=None)), "%Y-%m-%d %H:%M:%S.%f"
        )
        time_diff_minutes = (
            datetime.now() - method_time_creation
        ).total_seconds() // 60
        if time_diff_minutes > self.__METHOD_TIME_THRESHOLD:
            logger.warning(
                f"Image De-id Method {dsrc_method.data_source.id} failed. Please look for "
                f"job_processed_status {job_id_file_name}"
            )
            dsrc_method.set_as_error()
            return "Failed"

    def __create_json_job_process_content(
        self,
        destination_location,
        job_id_file_name,
        job_processed_status,
        processed_images_list,
        successful_processed_images,
    ):
        failed_data = self.__get_failed_images(processed_images_list)

        content = self.__read_content_from_object_key(
            f"{destination_location}/{job_id_file_name}"
        )

        data = {
            "job_processed_status": job_processed_status,
            "number_of_successes_images_processed": successful_processed_images,
            "number_of_failed_images_processed": len(failed_data),
            "failed_processed_data": failed_data,
            **content,
        }

        self.__update_job_process_file(
            data=data, key=f"{destination_location}/{job_id_file_name}"
        )

    def __get_failed_images(self, processed_images_list):
        failed_images = list()

        for file in processed_images_list:
            key = file["Key"]
            _, file_name, _, _ = break_s3_object(key)
            if re.search(r"^de-id.*\-fail.json", file_name):
                failed_images.append(file_name)

        return failed_images

    def __get_successful_images(self, processed_images_list):
        processed_images_count = 0

        for object_key in processed_images_list:
            key = object_key["Key"]
            _, file_name, _, _ = break_s3_object(key)
            if re.search(r"^de-id.*\-success.json", file_name):
                processed_images_count += 1

        return processed_images_count

    def __list_bucket_objects(self, destination_location):
        try:
            list_of_objects = self.__s3_client.list_objects(
                Bucket=self.__source_bucket, Prefix=destination_location
            )["Contents"]
            if list_of_objects:
                return list_of_objects
            raise EmptyBucketError(destination_location)
        except ClientError as e:
            raise EmptyBucketError(destination_location) from e

    def __read_content_from_object_key(self, key):
        try:
            bucket_to_read = self.__s3_resource.Bucket(self.__source_bucket)
            content = (
                bucket_to_read.Object(key)
                .get()["Body"]
                .read()
                .decode("utf8")
                .replace("'", '"')
            )
            if content:
                return json.loads(content)
            raise NoObjectContentError(key)
        except ClientError as e:
            raise NoObjectContentError(key, error=e)

    def __update_job_process_file(self, data, key):
        try:
            with BytesIO() as io_stream:
                io_stream.write(json.dumps(data).encode())
                io_stream.seek(0)

                self.__s3_client.put_object(
                    Body=io_stream, Bucket=self.__source_bucket, Key=key
                )
                logger.info(
                    f"Uploaded job process file to {key} at bucket {self.__source_bucket}"
                )
        except (BufferError, ClientError) as e:
            raise UpdateJobProcessError(self.__source_bucket, error=e)
