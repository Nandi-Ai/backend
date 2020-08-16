import logging

from mainapp.utils import aws_service

logger = logging.getLogger(__name__)


def delete_data_source_files_from_bucket(data_source, org_name):
    if data_source.dir:
        if data_source.dir == "":
            logger.warning(
                f"Warning: data source {data_source.name}:{data_source.id} "
                f"in dataset {data_source.dataset.name}:{data_source.dataset.id} "
                f"in org {data_source.dataset.organization.name}'dir' field is an empty string ('')"
            )
        else:  # delete dir in bucket
            s3_resource = aws_service.create_s3_resource(org_name=org_name)
            try:
                bucket = s3_resource.Bucket(data_source.bucket)
                bucket.objects.filter(Prefix=f"{data_source.dir}/").delete()
            except s3_resource.meta.client.exceptions.NoSuchKey:
                logger.warning(
                    f"Warning no such key {data_source.dir} in {data_source.bucket}. "
                    f"Ignoring deleting dir while deleting data_source {data_source.name}:{data_source.id} "
                    f"in org {data_source.dataset.organization.name}"
                )
            except s3_resource.meta.client.exceptions.NoSuchBucket:
                logger.warning(
                    f"Warning no such bucket {data_source.bucket} while trying to delete dir {dir}"
                )


def __delete_glue_tables_chunk(data_source, glue_client, database_name, max_results):
    response = glue_client.get_tables(
        DatabaseName=database_name, MaxResults=max_results
    )
    tables_list = response["TableList"]
    for table in tables_list:
        table_name = table["Name"]
        if (
            table_name == f"{data_source.dir}_full"
            or f"{data_source.dir}_limited" in table_name
        ):
            glue_client.delete_table(DatabaseName=database_name, Name=table_name)


def delete_data_source_glue_tables(data_source, org_name):
    if data_source.glue_table:
        glue_client = aws_service.create_glue_client(org_name=org_name)

        try:
            __delete_glue_tables_chunk(
                data_source=data_source,
                database_name=data_source.dataset.glue_database,
                glue_client=glue_client,
                max_results=100,
            )
            logger.info(
                f"Removed glue table: {data_source.glue_table} "
                f"for datasource {data_source.name}:{data_source.id} successfully "
                f"in dataset {data_source.dataset.name}:{data_source.dataset.id} "
                f"in org {data_source.dataset.organization.name}"
            )

        except glue_client.exceptions.EntityNotFoundException as e:
            logger.exception(
                f"Unexpected error when deleting glue table "
                f"for datasource {data_source.name}:{data_source.id}",
                e,
            )
