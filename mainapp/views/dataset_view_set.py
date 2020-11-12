import json
import logging
import os
import time
import uuid

from botocore.exceptions import ClientError
from http import HTTPStatus
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet


from mainapp.exceptions import (
    CreateRoleError,
    InvalidDatasetPermissions,
    PutPolicyError,
)
from mainapp.models import Activity, Execution, Method, Study
from mainapp.serializers import DatasetSerializer, MethodSerializer
from mainapp.settings import LYNX_FRONT_STATIC_BUCKET, LYNX_ORGANIZATION, ORG_VALUES

from mainapp.utils.aws_utils import (
    assume_role,
    create_role,
    generate_admin_s3_policy,
    put_policy,
    refresh_dataset_file_share_cache,
    TEMP_EXECUTION_DIR,
)
from mainapp.utils.aws_service import create_sts_client, create_s3_client
from mainapp.utils.deidentification.common.deid_helper_functions import handle_method
from mainapp.utils.lib import (
    create_glue_database,
    process_structured_data_sources_in_background,
    validate_file_type,
)
from mainapp.utils.monitoring import handle_event
from mainapp.utils.monitoring.monitor_events import MonitorEvents
from mainapp.utils.permissions import IsDatasetAdmin
from mainapp.utils.response_handler import (
    BadRequestErrorResponse,
    ConflictErrorResponse,
    ErrorResponse,
    ForbiddenErrorResponse,
)

logger = logging.getLogger(__name__)


class DatasetViewSet(ModelViewSet):
    http_method_names = ["get", "head", "post", "put", "delete"]
    serializer_class = DatasetSerializer
    permission_classes = [IsDatasetAdmin]
    filter_fields = ("ancestor",)
    file_types = {
        ".jpg": ["image/jpeg"],
        ".jpeg": ["image/jpeg"],
        ".tiff": ["image/tiff"],
        ".png": ["image/png"],
        ".bmp": ["image/bmp", "image/x-windows-bmp"],
    }

    def get_queryset(self):
        return self.request.user.datasets.exclude(is_deleted=True)

    def __generate_base_trust_relationship(self, dataset):
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": f"arn:aws:iam::{ORG_VALUES[dataset.organization.name]['ACCOUNT_NUMBER']}:root"
                    },
                    "Action": "sts:AssumeRole",
                    "Condition": dict(),
                }
            ],
        }

    # noinspection PyMethodMayBeStatic
    def __logic_validate(self, dataset_data):

        if dataset_data["state"] == "private":
            if "default_user_permission" not in dataset_data:
                raise InvalidDatasetPermissions(
                    "default_user_permission must be set since the state is private"
                )

            if not dataset_data["default_user_permission"]:
                raise InvalidDatasetPermissions(
                    "default_user_permission must be none or aggregated"
                )

    def __creation_logic_validate(self, dataset_data):
        self.__logic_validate(dataset_data)

        if dataset_data["state"] == "public" and dataset_data["aggregated_users"]:
            raise InvalidDatasetPermissions(
                "Dataset with public state can not have aggregated users"
            )

        if dataset_data["state"] == "archived":
            raise InvalidDatasetPermissions(
                "Can't create new dataset with status archived"
            )

    @action(detail=True, permission_classes=[IsAuthenticated], methods=["put"])
    def starred(self, request, *args, **kwargs):
        dataset = self.get_object()
        dataset.starred_users.add(request.user)
        dataset.save()

        return Response(DatasetSerializer(dataset).data)

    @action(detail=True, permission_classes=[IsAuthenticated], methods=["put"])
    def unstarred(self, request, *args, **kwargs):
        dataset = self.get_object()
        dataset.starred_users.remove(request.user)
        dataset.save()

        return Response(DatasetSerializer(dataset).data)

    @action(detail=True, permission_classes=[IsAuthenticated], methods=["get"])
    def execution_sts(self, request, pk=None):  # call from execution user
        dataset = self.get_object()
        if not request.user.is_execution:
            return BadRequestErrorResponse("Requested user is not an execution user !")
        execution = request.user.the_execution.last()

        try:
            query_id = request.query_params["query_execution_id"]
        except KeyError as e:
            return BadRequestErrorResponse(
                "Missing execution result id in request for sts", error=e
            )

        try:
            study = Study.objects.filter(execution=execution).last()
        except Study.DoesNotExist:
            return ErrorResponse("This is not the execution of any study")

        if study not in dataset.studies.all():
            return ForbiddenErrorResponse(
                "The user is not permitted to query this dataset"
            )

        org_name = study.organization.name
        sts_client = create_sts_client(org_name=org_name)

        role_to_assume = {
            "RoleArn": f"arn:aws:iam::{ORG_VALUES[org_name]['ACCOUNT_NUMBER']}:role/{dataset.iam_role}",
            "RoleSessionName": f"session_{uuid.uuid4()}",
            "DurationSeconds": 900,
            "Policy": json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": ["s3:GetObject", "s3:GetObjectAcl"],
                            "Resource": f"arn:aws:s3:::{dataset.full_path}/{TEMP_EXECUTION_DIR}/{query_id}.csv",
                        },
                        {"Effect": "Allow", "Action": ["kms:Decrypt"], "Resource": "*"},
                    ],
                }
            ),
        }

        try:
            response = sts_client.assume_role(**role_to_assume)
        except ClientError as e:
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=e,
            )

        return Response(
            {"aws_sts_creds": response["Credentials"], "bucket": dataset.bucket}
        )

    @action(detail=True, permission_classes=[IsDatasetAdmin], methods=["get"])
    def data_source_examples(self, request, *args, **kwargs):
        dataset = self.get_object()
        data_sources = dataset.data_sources
        data_source_examples = {
            str(data_source.id): data_source.example_values()
            for data_source in data_sources.iterator()
        }
        return Response(data_source_examples)

    @action(detail=True, methods=["get"])
    def methods(self, request, *args, **kwargs):
        dataset = self.get_object()

        return Response(MethodSerializer(dataset.methods, many=True).data, status=200)

    @action(detail=True, methods=["post"])
    def add_method(self, request, *args, **kwargs):
        dataset = self.get_object()

        if not dataset.data_sources.all():
            logger.error(
                f"Tried to add method for empty dataset {dataset.name}:{dataset.id}"
            )
            return BadRequestErrorResponse(
                "Dataset must have data sources in order to create a method"
            )

        logger.info(f"Validating Method for Dataset {dataset.name}:{dataset.id}")
        method_serialized = MethodSerializer(data=request.data)

        method_serialized.is_valid(raise_exception=True)

        for method in dataset.methods.all():
            if method.state == Method.PENDING:
                logger.warning(
                    "Aborting method creation due to running de identification process"
                )
                return BadRequestErrorResponse(
                    "Dataset is currently being De-identified, please try again later"
                )

        method = method_serialized.save()
        logger.debug(
            f"Created Method {method.name}:{method.id} for Dataset {dataset.name}:{dataset.id}"
        )

        logger.info(
            f"Deidentifying Data Sources according to Method {method.name}:{method.id}"
        )
        if method.data_source_methods:
            dsrc_index = 0
            for dsrc_method in method.data_source_methods.all():
                dsrc_index += 1
                try:
                    handle_method(dsrc_method, dsrc_method.data_source, dsrc_index)
                except Exception as e:
                    # If the code reached here, it means that an error was raised trying to create the method handler.
                    # Meaning data source methods will never be invoked, so the method is discarded.
                    method.delete()
                    return BadRequestErrorResponse(str(e))

        method.save()

        return Response(MethodSerializer(method).data, status=HTTPStatus.CREATED)

    @action(detail=True, methods=["get"])
    def get_write_sts(self, request, *args, **kwargs):
        dataset = self.get_object()

        try:
            sts_response = assume_role(
                org_name=dataset.organization.name, role_name=dataset.iam_role
            )
            logger.info(
                f"Generated STS credentials for Dataset: {dataset.name}:{dataset.id} in org {dataset.organization.name}"
            )
        except ClientError as e:
            message = f"Could not assume role for role_name {dataset.iam_role} with dataset_id {dataset.id}"
            logger.exception(message)
            return ErrorResponse(message=message, error=e)

        return Response(
            {
                "bucket": dataset.bucket,
                "region": dataset.region,
                "aws_sts_creds": sts_response["Credentials"],
                "location": dataset.bucket_dir,
            }
        )

    def __generate_permissions_activity(
        self, user, permission, dataset, real_user, action="grant"
    ):
        Activity.objects.create(
            type="dataset permission",
            dataset=dataset,
            user=real_user,
            meta={
                "user_affected": str(user.id),
                "action": action,
                "permission": permission,
            },
        )

    def __generate_permission_activities(
        self, users, permission, dataset, real_user, action="grant"
    ):
        for user in users:
            self.__generate_permissions_activity(
                user, permission, dataset, real_user, action
            )

    def __create_dataset_iam_role(self, dataset):
        try:
            create_role(
                org_name=dataset.organization.name,
                role_name=dataset.iam_role,
                assume_role_policy_document=json.dumps(
                    self.__generate_base_trust_relationship(dataset)
                ),
            )
        except ClientError as e:
            raise CreateRoleError(role_name=dataset.iam_role, error=e)

        try:
            put_policy(
                org_name=dataset.organization.name,
                role_name=dataset.iam_role,
                policy_name=dataset.iam_role,
                policy_document=generate_admin_s3_policy(
                    dataset.bucket_dir, dataset.bucket
                ),
            )
        except ClientError as e:
            raise PutPolicyError(
                policy_name=dataset.iam_role, role_name=dataset.iam_role, error=e
            )

    def __update_user_status(
        self, request_users, current_users, dataset, request, permission
    ):
        new_users = request_users - current_users
        removed_users = current_users - request_users

        for user in new_users:
            handle_event(
                MonitorEvents.EVENT_DATASET_ADD_USER,
                {
                    "dataset": dataset,
                    "view_request": request,
                    "additional_data": {
                        "user_list": user.display_name,
                        "permission": permission,
                    },
                },
            )
            self.__generate_permissions_activity(
                user, permission, dataset, request.user
            )

        for user in removed_users:
            handle_event(
                MonitorEvents.EVENT_DATASET_REMOVE_USER,
                {
                    "dataset": dataset,
                    "view_request": request,
                    "additional_data": {
                        "user_list": user.display_name,
                        "permission": permission,
                    },
                },
            )
            self.__generate_permissions_activity(
                user, permission, dataset, request.user, action="remove"
            )

    def __update_users(self, dataset_data, dataset, request):
        self.__update_user_status(
            set(dataset_data["admin_users"]),
            set(dataset.admin_users.all()),
            dataset,
            request,
            "admin",
        )

        self.__update_user_status(
            set(dataset_data["aggregated_users"]),
            set(dataset.aggregated_users.all()),
            dataset,
            request,
            "aggregated_access",
        )

        self.__update_user_status(
            set(dataset_data["full_access_users"]),
            set(dataset.full_access_users.all()),
            dataset,
            request,
            "full_access",
        )

    def __generate_create_activities(self, dataset, real_user):
        self.__generate_permission_activities(
            dataset.admin_users.all(), "admin", dataset, real_user
        )
        self.__generate_permission_activities(
            dataset.aggregated_users.all(), "aggregated_access", dataset, real_user
        )
        self.__generate_permission_activities(
            dataset.full_access_users.all(), "full_access", dataset, real_user
        )

    def create(self, request, **kwargs):
        dataset_serialized = self.serializer_class(
            data=request.data, context={"request": request}, allow_null=True
        )
        dataset_serialized.is_valid(raise_exception=True)

        try:
            self.__creation_logic_validate(dataset_serialized.validated_data)
        except InvalidDatasetPermissions as idp:
            return BadRequestErrorResponse(str(idp))

        dataset = dataset_serialized.save()

        org_name = dataset.organization.name
        s3_client = create_s3_client(org_name=org_name)

        try:
            s3_client.put_object(
                Bucket=dataset.bucket, Key=f"{dataset.bucket_dir}/", ACL="private"
            )

            create_glue_database(org_name=org_name, dataset=dataset)
            time.sleep(1)  # wait for the bucket to be created
        except ClientError as e:
            dataset.delete()
            return ErrorResponse(f"Could not create dataset {dataset.name}", error=e)

        try:
            self.__create_dataset_iam_role(dataset)
        except (CreateRoleError, PutPolicyError) as e:
            dataset.delete()
            return ErrorResponse("Failed to create IAM role", e)

        handle_event(
            MonitorEvents.EVENT_DATASET_CREATED,
            {"dataset": dataset, "view_request": request},
        )
        data = self.serializer_class(dataset, allow_null=True).data
        real_user = (
            request.user
            if not request.user.is_execution
            else Execution.objects.get(execution_user=request.user).real_user
        )

        self.__generate_create_activities(dataset, real_user)

        refresh_dataset_file_share_cache(org_name=org_name)

        return Response(data, status=HTTPStatus.CREATED)

    def update(self, request, *args, **kwargs):
        dataset_serialized = self.serializer_class(data=request.data, allow_null=True)
        dataset_serialized.is_valid(raise_exception=True)

        dataset_data = dataset_serialized.validated_data

        try:
            self.__logic_validate(dataset_data)
        except InvalidDatasetPermissions as idp:
            return BadRequestErrorResponse(str(idp))

        dataset = self.get_object()

        if dataset.has_pending_datasource():
            return ConflictErrorResponse(
                "Some of the data-sources are currently being processed. Please try again later"
            )

        self.__update_users(dataset_data, dataset, request)

        if dataset.cover != request.data["cover"]:
            if not request.data["cover"].lower().startswith("dataset/gallery"):
                file_name = request.data["cover"]
                workdir = "/tmp/"
                s3_client = create_s3_client(org_name=LYNX_ORGANIZATION)
                local_path = os.path.join(workdir, file_name)
                try:
                    validate_file_type(
                        s3_client=s3_client,
                        bucket=LYNX_FRONT_STATIC_BUCKET,
                        workdir="/tmp/dataset/",
                        object_key=file_name,
                        local_path=local_path,
                        file_types=self.file_types,
                    )
                except Exception as e:
                    return BadRequestErrorResponse(
                        "Validation for image failed", error=e
                    )

        result = super(self.__class__, self).update(request=self.request)

        updated_dataset = self.get_object()
        process_structured_data_sources_in_background(dataset=updated_dataset)

        return result

    def destroy(self, request, *args, **kwargs):
        dataset = self.get_object()

        def delete_dataset_tree(dataset_to_delete):
            for child_dataset in dataset_to_delete.children.all():
                delete_dataset_tree(child_dataset)
                child_dataset.delete()
            logger.info(
                f"All subsets were deleted for dataset {dataset_to_delete.name}:{dataset_to_delete.id} "
                f"in org {dataset_to_delete.organization.name}"
            )

        delete_tree_raw = request.GET.get("delete_tree")
        delete_tree = True if delete_tree_raw == "true" else False

        if delete_tree:
            delete_dataset_tree(dataset)

        elif dataset.ancestor:
            for child in dataset.children.all():
                child.ancestor = dataset.ancestor
                child.save()

        handle_event(
            MonitorEvents.EVENT_DATASET_DELETED,
            {"dataset": dataset, "view_request": request},
        )

        org_name = dataset.organization.name
        dataset.delete()

        refresh_dataset_file_share_cache(org_name=org_name)

        return Response(status=HTTPStatus.NO_CONTENT)
        # return super(self.__class__, self).destroy(request=self.request)
