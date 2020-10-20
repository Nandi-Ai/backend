import json
import logging
from datetime import datetime
from io import BytesIO

from botocore.exceptions import ClientError

from mainapp import settings
from mainapp.utils import lib, aws_service
from mainapp.utils.deidentification.common.image_de_id_exceptions import (
    LambdaInvocationError,
    UploadBatchProcessError,
    BaseImageDeIdError,
)

logger = logging.getLogger(__name__)


class ImageDeId(object):
    def __init__(self, org_name, data_source, dsrc_method):
        self.__org_name = org_name
        self.__data_source = data_source
        self.__dataset_id = data_source.dataset.id
        self.__dsrc_method = dsrc_method
        self.__job_id = str(dsrc_method.method.id)

        self.__destination_location = (
            f"{data_source.dir}/lynx-storage/deid_access_{dsrc_method.method.id}"
        )
        self.__source_bucket = data_source.dataset.bucket
        self.__job_id_file_name = f"{self.__data_source.id}-image-job-processing.json"
        self.__json_destination_bucket = f"{data_source.dir}/lynx-storage/status_deid_access_{dsrc_method.method.id}_json"

    def image_de_identification(self):
        try:
            uploading_batch_status_json = self.__generate_job_processing_update_status()

            self.__upload_job_update(job_process_json=uploading_batch_status_json)

            for image in self.__data_source.s3_objects:
                self.__invoke_deid_image_lambda(data_source=image)
        except BaseImageDeIdError as e:
            logger.exception(f"Failed to process De-id image", e)
            self.__dsrc_method.set_as_error()

    def delete(self):
        logger.info(
            f"Deleting method files for DataSourceMethod {self.__dsrc_method.method.id}"
        )

        # delete folder in bucket
        try:
            s3_resource = aws_service.create_s3_resource(org_name=self.__org_name)
            bucket = s3_resource.Bucket(self.__data_source.bucket)
            # this will actually not raise any error if location not found
            bucket.objects.filter(Prefix=self.__destination_location).delete()
        except ClientError as e:
            logger.exception(
                f"Error deleting method files for DataSourceMethod {self.__dsrc_method.method.id}",
                e,
            )

        # delete glue table
        glue_client = aws_service.create_glue_client(org_name=self.__org_name)
        try:
            glue_client.delete_table(
                DatabaseName=self.__data_source.dataset.glue_database,
                Name=self.__dsrc_method.get_glue_table(),
            )
        except glue_client.exceptions.EntityNotFoundException as e:
            logger.warning(
                f"Glue Table Not Found for DataSourceMethod {self.__dsrc_method.method.id}"
            )
        except Exception as e:
            logger.exception(
                f"Error deleting glue table for DataSourceMethod {self.__dsrc_method.method.id}",
                e,
            )

    def __generate_job_processing_update_status(self):
        logger.info(
            f"Creating json file for Image De-id job processing for "
            f"Method {self.__dsrc_method.method.name}:{self.__dsrc_method.method.id}"
            f"Data Source {self.__data_source.name}:{self.__data_source.id}"
        )
        image_s3_obj = self.__data_source.s3_objects[0]["key"]
        bucket_path, image_name, _, _ = lib.break_s3_object(image_s3_obj)

        job_status = "setup"
        date = str(datetime.now())
        upload_status = {
            "source_location": bucket_path,
            "destination_location": self.__destination_location,
            "created_date": date,
            "last_update": date,
            "job_status": job_status,
            "number_of_images_to_process": len(self.__data_source.s3_objects),
            "job_id": self.__job_id,
        }

        return upload_status

    def __upload_job_update(self, job_process_json):
        s3_client = aws_service.create_s3_client(org_name=self.__org_name)

        try:
            logger.info(
                f"Uploading json file for Image De-id job processing for "
                f"Method {self.__dsrc_method.method.name}:{self.__dsrc_method.method.id}"
                f"Data Source {self.__data_source.name}:{self.__data_source.id}"
            )
            with BytesIO() as write_stream:
                write_stream.write(json.dumps(job_process_json).encode())
                write_stream.seek(0)

                s3_client.upload_fileobj(
                    write_stream,
                    self.__source_bucket,
                    f"{self.__json_destination_bucket}/{self.__job_id_file_name}",
                )
        except (BufferError, ClientError) as e:
            raise UploadBatchProcessError(
                f"{self.__json_destination_bucket}/{self.__job_id_file_name}", error=e
            )

    def __invoke_deid_image_lambda(self, data_source):
        image_s3_obj = data_source["key"]
        bucket_path, image_name, _, _ = lib.break_s3_object(image_s3_obj)

        payload = {
            "source_bucket": self.__source_bucket,
            "image_s3_obj": image_s3_obj,
            "destination_bucket": self.__destination_location,
            "json_destination_bucket": self.__json_destination_bucket,
            "input_image_name": image_name,
        }

        lambda_client = aws_service.create_lambda_client(org_name=self.__org_name)

        logger.info(
            f"Invoking Lambda function for image_object {image_s3_obj} and image_name {image_name} "
            f"Method {self.__dsrc_method.method.name}:{self.__dsrc_method.method.id}"
            f"Data Source {self.__data_source.name}:{self.__data_source.id}"
        )

        try:
            lambda_client.invoke(
                FunctionName=settings.IMAGE_DE_ID_FUNCTION_NAME,
                InvocationType="Event",
                LogType="Tail",
                Payload=bytes(json.dumps(payload), "utf-8"),
            )
        except ClientError as e:
            raise LambdaInvocationError(
                image_name,
                image_s3_obj,
                self.__destination_location,
                self.__dsrc_method.method.name,
                self.__dsrc_method.method.id,
                error=e,
            )
