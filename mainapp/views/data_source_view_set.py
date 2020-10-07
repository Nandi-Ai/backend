import json
import logging
import os
import shutil
import threading

import pyreadstat
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

# noinspection PyPackageRequirements
from slugify import slugify

from mainapp.models import Execution
from mainapp.serializers import DataSourceSerializer
from mainapp.utils import statistics, lib, aws_service
from mainapp.utils.aws_utils import s3_storage
from mainapp.utils.lib import process_structured_data_source_in_background
from mainapp.utils.monitoring import handle_event, MonitorEvents
from mainapp.utils.response_handler import (
    ErrorResponse,
    ForbiddenErrorResponse,
    NotFoundErrorResponse,
    BadRequestErrorResponse,
)

logger = logging.getLogger(__name__)


class DataSourceViewSet(ModelViewSet):
    serializer_class = DataSourceSerializer
    http_method_names = ["get", "head", "post", "put", "delete"]
    filter_fields = ("dataset",)
    file_types = {
        ".jpg": ["image/jpeg"],
        ".jpeg": ["image/jpeg"],
        ".tiff": ["image/tiff"],
        ".png": ["image/png"],
        ".csv": ["application/csv", "text/csv", "text/plain"],
        ".sav": ["application/octet-stream"],
        ".zsav": [],
        ".zip": ["application/zip"],
        ".xml": ["text/html", "text/xml"],
    }

    @action(detail=True, methods=["get"])
    def statistics(self, request, *args, **kwargs):
        data_source = self.get_object()
        if data_source.is_ready():
            return ErrorResponse(
                f"Data is still in processing. Datasource id {data_source.name}",
                status_code=503,
            )

        query_from_front = request.query_params.get("grid_filter")
        if query_from_front:
            query_from_front = json.loads(query_from_front)

        final_result, response = lib.calculate_statistics(
            data_source, query_from_front=query_from_front
        )

        if request.user in data_source.dataset.aggregated_users.all():
            max_rows_after_filter = statistics.max_count(response)
            if max_rows_after_filter < 100:
                return ForbiddenErrorResponse(
                    "Sorry, we can not show you the results, the cohort is too small"
                )

        return Response(final_result)

    def get_queryset(self):
        return self.request.user.data_sources

    def create(self, request, *args, **kwargs):
        ds_types = ["structured", "images", "zip", "xml"]
        data_source_serialized = self.serializer_class(
            data=request.data, allow_null=True
        )

        if data_source_serialized.is_valid():
            data_source_data = data_source_serialized.validated_data
            dataset = data_source_data["dataset"]

            user = (
                request.user
                if not request.user.is_execution
                else Execution.objects.get(execution_user=request.user).real_user
            )
            if dataset not in user.datasets.all():
                return BadRequestErrorResponse(
                    f"Dataset {dataset.id} does not exist or does not belong to the user"
                )

            if data_source_data["type"] not in ds_types:
                return BadRequestErrorResponse(
                    f"Data s3_dir type must be one of: {ds_types}"
                )

            if "s3_objects" in data_source_data:
                if not isinstance(data_source_data["s3_objects"], list):
                    return ForbiddenErrorResponse("s3 objects must be a (json) list")

            if data_source_data["type"] in ["zip", "structured" "xml"]:
                if "s3_objects" not in data_source_data:
                    logger.exception("s3_objects field must be included")

                if len(data_source_data["s3_objects"]) != 1:
                    return BadRequestErrorResponse(
                        f"Data s3_dir of type {data_source_data['type']} "
                        f"structured and zip must include exactly one item in s3_objects json array"
                    )

                s3_obj = data_source_data["s3_objects"][0]["key"]
                path, file_name, file_name_no_ext, ext = lib.break_s3_object(s3_obj)
                if ext not in ["sav", "zsav", "csv", "xml"]:
                    return BadRequestErrorResponse(
                        "File type is not supported as a structured data s3_dir"
                    )
            data_source = data_source_serialized.save()
            data_source.set_as_pending()
            s3_obj = data_source.s3_objects[0]["key"]
            _, file_name, _, _ = lib.break_s3_object(s3_obj)
            workdir = f"/tmp/{data_source.id}"
            s3_client = aws_service.create_s3_client(org_name=dataset.organization.name)
            local_path = os.path.join(workdir, file_name)
            try:
                lib.validate_file_type(
                    s3_client,
                    data_source.dataset.bucket,
                    workdir,
                    s3_obj,
                    local_path,
                    self.file_types,
                )
            except Exception:
                data_source.set_as_error()
                return Response(
                    self.serializer_class(data_source, allow_null=True).data, status=201
                )

            data_source.programmatic_name = (
                slugify(data_source.name) + "-" + str(data_source.id).split("-")[0]
            )
            data_source.save()

            if data_source.type == "structured":
                # set initial glue table value
                data_source.glue_table = data_source.dir.translate(
                    {ord(c): "_" for c in "!@#$%^&*()[]{};:,./<>?\|`~-=_+\ "}
                ).lower()
                s3_obj = data_source.s3_objects[0]["key"]
                path, file_name, file_name_no_ext, ext = lib.break_s3_object(s3_obj)

                if ext in ["sav", "zsav"]:  # convert to csv
                    s3_client = aws_service.create_s3_client(
                        org_name=data_source.dataset.organization.name
                    )
                    workdir = f"/tmp/{data_source.id}"
                    os.makedirs(workdir)
                    try:
                        s3_storage.download_file(
                            s3_client=s3_client,
                            bucket_name=data_source.dataset.bucket,
                            s3_object=s3_obj,
                            file_path=os.path.join(workdir, file_name),
                        )
                    except Exception as e:
                        return ErrorResponse(
                            f"There was an error to download the file {file_name} with error",
                            error=e,
                        )

                    df, meta = pyreadstat.read_sav(workdir + "/" + file_name)
                    csv_path_and_file = workdir + "/" + file_name_no_ext + ".csv"
                    df.to_csv(csv_path_and_file)
                    try:
                        s3_storage.upload_file(
                            s3_client=s3_client,
                            csv_path_and_file=csv_path_and_file,
                            bucket_name=data_source.dataset.bucket,
                            file_path=os.path.join(path, f"{file_name_no_ext}.csv"),
                        )
                    except Exception as e:
                        return ErrorResponse(
                            f"There was an error to upload the file {file_name} with error",
                            error=e,
                        )
                    data_source.s3_objects.pop()
                    data_source.s3_objects.append(
                        {
                            "key": path + "/" + file_name_no_ext + ".csv",
                            "size": os.path.getsize(csv_path_and_file),
                        }
                    )
                    shutil.rmtree(workdir)

                try:
                    if request.data["is_column_present"]:
                        lib.check_csv_for_empty_columns(
                            org_name=dataset.organization.name, data_source=data_source
                        )
                except Exception as e:
                    dataset.save()
                    return BadRequestErrorResponse(
                        f"There was an error to when tried to check column name in data_source {data_source.name} "
                        f"and data_source_id {data_source.id}",
                        error=e,
                    )

                process_structured_data_source_in_background(
                    org_name=dataset.organization.name, data_source=data_source
                )

            elif data_source.type == "zip":
                handle_zip_thread = threading.Thread(
                    target=lib.handle_zipped_data_source,
                    args=[data_source, request.user.organization.name],
                )
                handle_zip_thread.start()

            else:
                # not structured
                lib.update_folder_hierarchy(
                    data_source=data_source,
                    org_name=data_source.dataset.organization.name,
                )
                data_source.set_as_ready()

            handle_event(
                MonitorEvents.EVENT_DATASET_ADD_DATASOURCE,
                {"datasource": data_source, "view_request": request},
            )

            return Response(
                self.serializer_class(data_source, allow_null=True).data, status=201
            )

        else:
            return BadRequestErrorResponse(data_source_serialized.errors)

    def update(self, request, *args, **kwargs):
        serialized = self.serializer_class(data=request.data, allow_null=True)

        if serialized.is_valid():  # if not valid super will handle it
            dataset = serialized.validated_data["dataset"]
            # TODO to check if that even possible since the get_queryset should already handle filtering it..
            # TODO if does can remove the update method
            if dataset not in request.user.datasets.all():
                return NotFoundErrorResponse(
                    f"Dataset {dataset} does not exist or does not belong to the user"
                )

        return super(self.__class__, self).update(request=self.request)

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
