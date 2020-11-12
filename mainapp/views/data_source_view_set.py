from botocore.exceptions import ClientError

import json
import logging
import os
import pyreadstat
import threading

from http import HTTPStatus
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

# noinspection PyPackageRequirements
from slugify import slugify
from tempfile import TemporaryDirectory

from mainapp.exceptions import InvalidDataSourceError
from mainapp.models import DataSource, Method
from mainapp.serializers import (
    DataSourceSerializer,
    DataSourceColumnsSerializer,
    ReadStsSerializer,
)
from mainapp.utils.aws_service import create_s3_client
from mainapp.utils.aws_utils import (
    assume_role,
    download_file,
    generate_read_s3_policy,
    refresh_dataset_file_share_cache,
    upload_file,
)
from mainapp.utils.deidentification import DeidentificationError
from mainapp.utils.deidentification.common.deid_helper_functions import handle_method
from mainapp.utils.lib import (
    break_s3_object,
    calculate_statistics,
    check_csv_for_empty_columns,
    handle_zipped_data_source,
    LYNX_STORAGE_DIR,
    process_structured_data_source_in_background,
    PrivilegePath,
    update_folder_hierarchy,
    validate_file_type,
)
from mainapp.utils.monitoring import handle_event, MonitorEvents
from mainapp.utils.permissions import IsDataSourceAdmin
from mainapp.utils.response_handler import (
    ErrorResponse,
    ForbiddenErrorResponse,
    NotFoundErrorResponse,
    BadRequestErrorResponse,
)
from mainapp.utils.statistics import max_count

logger = logging.getLogger(__name__)


class DataSourceViewSet(ModelViewSet):
    serializer_class = DataSourceSerializer
    http_method_names = ["get", "head", "post", "put", "delete"]
    permission_classes = [IsDataSourceAdmin]
    filter_fields = ("dataset",)
    file_types = {
        ".jpg": ["image/jpeg"],
        ".jpeg": ["image/jpeg"],
        ".tiff": ["image/tiff"],
        ".png": ["image/png"],
        ".bmp": ["image/bmp", "image/x-windows-bmp"],
        ".csv": ["application/csv", "text/csv", "text/plain"],
        ".sav": ["application/octet-stream"],
        ".zsav": [],
        ".zip": ["application/zip"],
        ".xml": ["text/html", "text/xml"],
    }
    DS_TYPES = [
        DataSource.STRUCTURED,
        DataSource.IMAGES,
        DataSource.ZIP,
        DataSource.XML,
    ]

    def __validate_data_source_file(self, data_source):
        s3_obj = data_source.s3_objects[0]["key"]
        _, file_name, _, _ = break_s3_object(s3_obj)
        workdir = f"/tmp/{data_source.id}"
        s3_client = create_s3_client(org_name=data_source.dataset.organization.name)
        local_path = os.path.join(workdir, file_name)
        validate_file_type(
            s3_client,
            data_source.dataset.bucket,
            workdir,
            s3_obj,
            local_path,
            self.file_types,
        )

    def __validate_new_data_source(self, data_source_data):
        if data_source_data["type"] not in self.DS_TYPES:
            raise InvalidDataSourceError(
                BadRequestErrorResponse(
                    f"Data s3_dir type must be one of: {self.DS_TYPES}"
                )
            )

        if data_source_data.get("s3_objects"):
            if not isinstance(data_source_data["s3_objects"], list):
                raise InvalidDataSourceError(
                    BadRequestErrorResponse("s3 objects must be a (json) list")
                )

        if data_source_data.get("type") in [
            DataSource.ZIP,
            DataSource.STRUCTURED,
            DataSource.XML,
        ]:
            if not data_source_data.get("s3_objects"):
                logger.exception("s3_objects field must be included")

            if len(data_source_data["s3_objects"]) != 1:
                raise InvalidDataSourceError(
                    BadRequestErrorResponse(
                        f"Data s3_dir of type {data_source_data['type']} "
                        f"structured and zip must include exactly one item in s3_objects json array"
                    )
                )

            s3_obj = data_source_data["s3_objects"][0]["key"]
            path, file_name, file_name_no_ext, ext = break_s3_object(s3_obj)
            if ext not in ["sav", "zsav", "csv", "xml"]:
                raise InvalidDataSourceError(
                    BadRequestErrorResponse(
                        "File type is not supported as a structured data s3_dir"
                    )
                )

    def __process_structured(self, data_source, request):
        dataset = data_source.dataset
        org_name = dataset.organization.name

        data_source.glue_table = data_source.dir.translate(
            {ord(c): "_" for c in "!@#$%^&*()[]{};:,./<>?\|`~-=_+\ "}
        ).lower()

        s3_obj = data_source.s3_objects[0]["key"]
        path, file_name, file_name_no_ext, ext = break_s3_object(s3_obj)

        if ext in ["sav", "zsav"]:  # convert to csv
            s3_client = create_s3_client(org_name=org_name)
            workdir = TemporaryDirectory(data_source.id)
            local_path = os.path.join(workdir.name, file_name)
            try:
                try:
                    download_file(
                        s3_client=s3_client,
                        bucket_name=data_source.dataset.bucket,
                        s3_object=s3_obj,
                        file_path=local_path,
                    )
                except ClientError as e:
                    raise InvalidDataSourceError(
                        ErrorResponse(
                            f"There was an error to download the file {file_name} with error",
                            error=e,
                        )
                    )

                df, meta = pyreadstat.read_sav(f"{workdir.name}/{file_name}")
                csv_path_and_file = f"{workdir.name}/{file_name_no_ext}.csv"
                df.to_csv(csv_path_and_file)
                try:
                    upload_file(
                        s3_client=s3_client,
                        csv_path_and_file=csv_path_and_file,
                        bucket_name=data_source.dataset.bucket,
                        file_path=os.path.join(path, f"{file_name_no_ext}.csv"),
                    )
                except ClientError as e:
                    raise InvalidDataSourceError(
                        ErrorResponse(
                            f"There was an error to upload the file {file_name} with error",
                            error=e,
                        )
                    )

                data_source.s3_objects.pop()
                data_source.s3_objects.append(
                    {
                        "key": f"{path}/{file_name_no_ext}.csv",
                        "size": os.path.getsize(csv_path_and_file),
                    }
                )
            finally:
                workdir.cleanup()

        try:
            if request.data["is_column_present"]:
                check_csv_for_empty_columns(org_name=org_name, data_source=data_source)
        except ClientError as e:
            dataset.save()
            raise InvalidDataSourceError(
                BadRequestErrorResponse(
                    f"There was an error to when tried to check column name in data_source {data_source.name} "
                    f"and data_source_id {data_source.id}",
                    error=e,
                )
            )

        process_structured_data_source_in_background(
            org_name=org_name, data_source=data_source
        )

    def __process_zip(self, data_source, request):
        handle_zip_thread = threading.Thread(
            target=handle_zipped_data_source,
            args=[data_source, request.user.organization.name],
        )
        handle_zip_thread.start()

    def __process_other(self, data_source, *args):
        update_folder_hierarchy(
            data_source=data_source, org_name=data_source.dataset.organization.name
        )
        data_source.set_as_ready()

    @action(detail=True, permission_classes=[IsAuthenticated], methods=["get"])
    def statistics(self, request, *args, **kwargs):
        data_source = self.get_object()
        if not data_source.is_ready():
            return ErrorResponse(
                f"Data is still in processing. Datasource id {data_source.name}",
                status_code=HTTPStatus.CONFLICT,
            )

        query_from_front = request.query_params.get("grid_filter")
        if query_from_front:
            query_from_front = json.loads(query_from_front)

        final_result, response = calculate_statistics(
            data_source, query_from_front=query_from_front
        )

        if request.user in data_source.dataset.aggregated_users.all():
            max_rows_after_filter = max_count(response)
            if max_rows_after_filter < 100:
                return ForbiddenErrorResponse(
                    "Sorry, we can not show you the results, the cohort is too small"
                )

        return Response(final_result)

    @action(detail=True, methods=["get"])
    def example(self, request, *args, **kwargs):
        data_source = self.get_object()
        return Response({str(data_source.id): data_source.example_values()})

    @action(detail=True, permission_classes=[IsAuthenticated], methods=["get"])
    def get_read_sts(self, request, *args, **kwargs):
        data_source = self.get_object()
        dataset = data_source.dataset
        org_name = dataset.organization.name

        if data_source.type not in [DataSource.XML, DataSource.IMAGES]:
            logger.warning(
                f"Can not have sts permission for this data-source {data_source.name}:{data_source.id}"
            )
            return ForbiddenErrorResponse(
                "You can't have sts permission for this data-source"
            )

        permission_serialized_data = ReadStsSerializer(data=request.query_params)
        permission_serialized_data.is_valid(raise_exception=True)
        permission = permission_serialized_data.validated_data.get("permission")
        user_permission = self.__get_image_permission(
            permission, request.user, data_source, dataset
        )

        try:
            requested_dir = (
                f"{user_permission['permission']}_{user_permission['key']}"
                if user_permission.get("key")
                else user_permission["permission"]
            )

            sts_response = assume_role(
                org_name=org_name,
                role_name=dataset.iam_role,
                policy=generate_read_s3_policy(
                    bucket=dataset.bucket,
                    path=f"{dataset.bucket_dir}/{data_source.dir}/{LYNX_STORAGE_DIR}/{requested_dir}",
                ),
            )
            logger.info(
                f"Generated STS credentials for Dataset: {dataset.name}:{dataset.id} in org {dataset.organization.name}"
            )
        except ClientError as e:
            return ErrorResponse(
                message=f"Could not assume role for role_name {dataset.iam_role} with dataset_id {dataset.id}",
                error=e,
            )

        return Response(
            {
                "bucket": dataset.bucket,
                "region": dataset.region,
                "aws_sts_creds": sts_response["Credentials"],
                "location": data_source.get_location(user_permission),
            }
        )

    @action(detail=True, methods=["put"])
    def columns(self, request, *args, **kwargs):
        data_source = self.get_object()

        if data_source.type != DataSource.STRUCTURED:
            logger.error(
                f"Data Source {data_source.name}:{data_source.id} was sent to 'columns' endpoint even "
                f"though it isn't structured"
            )
            return BadRequestErrorResponse(
                "Only structured data sources support column updates"
            )

        for method in data_source.methods.all():
            if method.state == Method.PENDING:
                logger.warning(
                    "Aborting column configuration due to running de identification process"
                )
                return BadRequestErrorResponse(
                    "Data source is currently being De-identified, please try again later"
                )

        logger.info(f"Validating columns for {data_source.name}:{data_source.id}")
        columns_serialized = DataSourceColumnsSerializer(
            data={"columns": request.data}, context={"data_source": data_source}
        )

        columns_serialized.is_valid(raise_exception=True)

        logger.info(
            f"Fetching methods for Data Source {data_source.name}:{data_source.id}"
        )
        data_source.columns = request.data
        data_source.save()

        changed_columns = columns_serialized.get_changed_columns()
        if data_source.methods:
            dsrc_index = 0
            for dsrc_method in data_source.methods.all():
                dsrc_index += 1
                try:
                    if any([col in dsrc_method.attributes for col in changed_columns]):
                        handle_method(dsrc_method, data_source, dsrc_index)
                except DeidentificationError as de:
                    return BadRequestErrorResponse(str(de))
                except Exception:
                    dsrc_method.set_as_error()

        return Response(
            self.serializer_class(data_source, allow_null=True).data,
            status=HTTPStatus.CREATED,
        )

    def get_queryset(self):
        return self.request.user.data_sources

    def create(self, request, *args, **kwargs):
        data_process_mapping = {
            DataSource.STRUCTURED: self.__process_structured,
            DataSource.ZIP: self.__process_zip,
        }
        data_source_serialized = self.serializer_class(
            data=request.data, allow_null=True
        )

        data_source_serialized.is_valid(raise_exception=True)

        try:
            self.__validate_new_data_source(data_source_serialized.validated_data)
        except InvalidDataSourceError as e:
            return e.error_response

        data_source = data_source_serialized.save()
        data_source.set_as_pending()

        try:
            self.__validate_data_source_file(data_source)
        except Exception:
            data_source.set_as_error()
            return Response(
                self.serializer_class(data_source, allow_null=True).data,
                status=HTTPStatus.CREATED,
            )

        data_source.programmatic_name = (
            slugify(data_source.name) + "-" + str(data_source.id).split("-")[0]
        )
        data_source.save()

        try:
            data_process_mapping.get(data_source.type, self.__process_other)(
                data_source, request
            )
        except InvalidDataSourceError as e:
            return e.error_response

        handle_event(
            MonitorEvents.EVENT_DATASET_ADD_DATASOURCE,
            {"datasource": data_source, "view_request": request},
        )

        return Response(
            self.serializer_class(data_source, allow_null=True).data,
            status=HTTPStatus.CREATED,
        )

    def update(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = self.get_serializer(instance, data=request.data)
        serializer.is_valid(raise_exception=True)

        dataset = serializer.validated_data["dataset"]
        # TODO to check if that even possible since the get_queryset should already handle filtering it..
        # TODO if does can remove the update method
        if dataset not in request.user.datasets.all():
            return NotFoundErrorResponse(
                f"Dataset {dataset} does not exist or does not belong to the user"
            )

        refresh_dataset_file_share_cache(org_name=dataset.organization.name)
        return super(self.__class__, self).update(request=self.request)

    def __get_image_permission(self, permission, user, data_source, dataset):
        db_access = dataset.calc_access_to_database(user)
        db_access["permission"] = db_access["permission"].replace(" ", "_")
        db_permission = db_access["permission"]

        if data_source.type == DataSource.STRUCTURED:
            return db_access

        permission = permission or db_permission

        if permission in [
            PrivilegePath.LIMITED.value,
            PrivilegePath.AGG_STATS.value,
            PrivilegePath.FULL.value,
        ]:
            return {"permission": PrivilegePath.FULL.value, "key": None}
        elif permission == PrivilegePath.DEID.value:
            return {
                "permission": PrivilegePath.DEID.value,
                "key": str(
                    data_source.methods.get(data_source_id=data_source.id).method_id
                ),
            }
        return db_access

    # def destroy(self, request, *args, **kwargs): #now handling by a signal
    #     data_source = self.get_object()
    #
    #     if data_source.glue_table:
    #         # additional validations only for update:
    #         try:
    #             glue_client = aws_service.create_glue_client(settings.AWS['AWS_REGION'])
    #             glue_client.delete_table(
    #                 DatabaseName=data_source.dataset.glue_database,
    #                 Name=data_source.glue_table
    #             )
    #         except Exception as e:
    #             pass
    #
    #     return super(self.__class__, self).destroy(request=self.request)
