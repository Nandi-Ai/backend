import json
import logging
import os
import time
import uuid

import botocore.exceptions
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

# noinspection PyPackageRequirements
from slugify import slugify

from mainapp import resources, settings
from mainapp.exceptions.s3 import TooManyBucketsException
from mainapp.models import User, Dataset, Tag, Execution, Activity, DatasetUser
from mainapp.serializers import DatasetSerializer
from mainapp.utils import lib, aws_service
from mainapp.utils.lib import process_structured_data_sources_in_background
from mainapp.utils.monitoring import handle_event
from mainapp.utils.monitoring.monitor_events import MonitorEvents
from mainapp.utils.response_handler import (
    ErrorResponse,
    ForbiddenErrorResponse,
    BadRequestErrorResponse,
)

logger = logging.getLogger(__name__)


class DatasetViewSet(ModelViewSet):
    http_method_names = ["get", "head", "post", "put", "delete"]
    serializer_class = DatasetSerializer
    filter_fields = ("ancestor",)
    file_types = {
        ".jpg": ["image/jpeg"],
        ".jpeg": ["image/jpeg"],
        ".tiff": ["image/tiff"],
        ".png": ["image/png"],
        ".bmp": ["image/bmp"],
    }

    # noinspection PyMethodMayBeStatic
    def logic_validate(
        self, request, dataset_data
    ):  # only common validations for create and update! #

        if dataset_data["state"] == "private":
            if "default_user_permission" not in dataset_data:
                return BadRequestErrorResponse(
                    "default_user_permission must be set since the state is private"
                )

            if not dataset_data["default_user_permission"]:
                return BadRequestErrorResponse(
                    "default_user_permission must be none or aggregated"
                )

    @action(detail=True, methods=["put"])
    def starred(self, request, *args, **kwargs):
        dataset = self.get_object()
        dataset.starred_users.add(request.user)
        dataset.save()

        return Response(DatasetSerializer(dataset).data, status=200)

    @action(detail=True, methods=["put"])
    def unstarred(self, request, *args, **kwargs):
        dataset = self.get_object()
        dataset.starred_users.remove(request.user)
        dataset.save()

        return Response(DatasetSerializer(dataset).data, status=200)

    def get_queryset(self):
        return self.request.user.datasets.exclude(is_deleted=True)

    def create(self, request, **kwargs):
        dataset_serialized = self.serializer_class(data=request.data, allow_null=True)

        if dataset_serialized.is_valid():
            # create the dataset instance:
            # TODO maybe use super() as in update instead of completing the all process.

            dataset_data = dataset_serialized.validated_data

            # validations common for create and update:
            error_response = self.logic_validate(request, dataset_data)
            if error_response:
                return error_response

            # additional validation only for create:
            if dataset_data["state"] == "public" and dataset_data["aggregated_users"]:
                return BadRequestErrorResponse(
                    "Dataset with public state can not have aggregated users"
                )

            if dataset_data["state"] == "archived":
                return BadRequestErrorResponse(
                    "Can't create new dataset with status archived"
                )

            dataset = Dataset(
                name=dataset_data["name"],
                is_discoverable=dataset_data["is_discoverable"],
                cover=dataset_data.get("cover"),
                organization=(
                    dataset_data["ancestor"].organization
                    if "ancestor" in dataset_data
                    else request.user.organization
                ),
            )
            dataset.id = uuid.uuid4()

            # aws stuff
            org_name = dataset_data.get("ancestor", request.user).organization.name
            s3 = aws_service.create_s3_client(org_name=org_name)

            try:
                lib.create_s3_bucket(
                    org_name=org_name, name=dataset.bucket, s3_client=s3
                )
            except TooManyBucketsException as e:
                return ErrorResponse(error=e)
            except botocore.exceptions.ClientError as e:
                error = Exception(
                    f"Could not create s3 bucket {dataset.bucket} with following error"
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Could not create following dataset {dataset.name}", error=error
                )

            try:
                lib.set_policy_clear_athena_history(
                    s3_bucket=dataset.bucket, s3_client=s3
                )
            except botocore.exceptions.ClientError as e:
                error = Exception(
                    f"The bucket {dataset.bucket} does not exist or the user does not have the relevant permissions"
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )
            except s3.exceptions.NoSuchLifecycleConfiguration:
                return ForbiddenErrorResponse(
                    "The lifecycle configuration does not exist"
                )
            except Exception as e:
                error = Exception(
                    f"There was an error setting the lifecycle policy for the dataset bucket with error"
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )

            try:
                lib.create_glue_database(org_name=org_name, dataset=dataset)
                time.sleep(1)  # wait for the bucket to be created
            except botocore.exceptions.ClientError as e:
                error = Exception(
                    f"Could not create glue client with following error"
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Could not create dataset {dataset.name}", error=error
                )

            cors_configuration = {
                "CORSRules": [
                    {
                        "AllowedHeaders": ["*"],
                        "AllowedMethods": ["GET", "PUT", "POST", "DELETE"],
                        "AllowedOrigins": ["*"],
                        "ExposeHeaders": ["ETag"],
                        "MaxAgeSeconds": 3000,
                    }
                ]
            }

            try:
                s3.put_bucket_cors(
                    Bucket=dataset.bucket, CORSConfiguration=cors_configuration
                )
            except Exception as e:
                error = Exception(
                    f"Couldn't put bucket {dataset.bucket} policy for current {dataset.bucket} bucket"
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )

            # create the dataset policy:
            policy_json = {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": []}],
            }

            policy_json["Statement"][0]["Resource"].append(
                "arn:aws:s3:::" + dataset.bucket + "*"
            )
            client = aws_service.create_iam_client(org_name=org_name)

            policy_name = f"lynx-dataset-{dataset.id}"

            try:
                response = client.create_policy(
                    PolicyName=policy_name, PolicyDocument=json.dumps(policy_json)
                )
            except s3.exceptions.AccessDeniedException as e:
                error = Exception(
                    f"The user does not have needed permissions to create this policy: {policy_name}"
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )
            except Exception as e:
                error = Exception(
                    f"There was an error when trying to create this policy {policy_name}"
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )

            policy_arn = response["Policy"]["Arn"]

            # create the dataset role:
            role_name = "lynx-dataset-" + str(dataset.id)
            trust_policy_json = resources.create_base_trust_relationship(org_name)
            try:
                client.create_role(
                    RoleName=role_name,
                    AssumeRolePolicyDocument=json.dumps(trust_policy_json),
                    Description=policy_name,
                    MaxSessionDuration=43200,
                )
            except Exception as e:
                error = Exception(
                    f"The server can't process your request due to unexpected internal error"
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )

            try:
                client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
            except Exception as e:
                error = Exception(
                    f"The server can't process your request due to unexpected internal error"
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )

            dataset.save()

            dataset.description = dataset_data["description"]
            dataset.readme = dataset_data["readme"]
            req_admin_users = dataset_data["admin_users"]
            dataset.admin_users.set(
                list(User.objects.filter(id__in=[x.id for x in req_admin_users]))
            )
            req_aggregated_users = dataset_data["aggregated_users"]
            dataset.aggregated_users.set(
                list(User.objects.filter(id__in=[x.id for x in req_aggregated_users]))
            )
            req_full_access_users = dataset_data["full_access_users"]
            dataset.full_access_users.set(
                list(User.objects.filter(id__in=[x.id for x in req_full_access_users]))
            )

            req_users = dataset_data["datasetuser_set"]
            for dataset_user in req_users:
                DatasetUser.objects.create(dataset=dataset, **dataset_user)

            dataset.state = dataset_data["state"]
            dataset.default_user_permission = dataset_data["default_user_permission"]
            dataset.permission_attributes = dataset_data.get(
                "permission_attributes", None
            )
            req_tags = dataset_data["tags"]
            dataset.tags.set(Tag.objects.filter(id__in=[x.id for x in req_tags]))
            dataset.user_created = request.user
            dataset.ancestor = (
                dataset_data["ancestor"] if "ancestor" in dataset_data else None
            )
            if "ancestor" in dataset_data:
                dataset.is_discoverable = False
                dataset.state = "private"
                # for public dataset case
                if dataset.default_user_permission is None:
                    dataset.default_user_permission = "None"
                    dataset.full_access_users.add(request.user.id)
                # for private dataset case with aggregated_access permission
                if dataset.default_user_permission == "aggregated_access":
                    dataset.aggregated_users.add(request.user.id)

            # dataset.bucket = 'lynx-dataset-' + str(dataset.id)
            dataset.programmatic_name = (
                slugify(dataset.name) + "-" + str(dataset.id).split("-")[0]
            )

            dataset.save()
            handle_event(
                MonitorEvents.EVENT_DATASET_CREATED,
                {"dataset": dataset, "view_request": request},
            )

            # the role takes this time to be created!
            # it is here in order to prevent calling GetDatasetSTS before creation
            time.sleep(8)

            data = self.serializer_class(dataset, allow_null=True).data

            real_user = (
                request.user
                if not request.user.is_execution
                else Execution.objects.get(execution_user=request.user).real_user
            )

            # activity:
            for user in dataset.admin_users.all():
                Activity.objects.create(
                    type="dataset permission",
                    dataset=dataset,
                    user=real_user,
                    meta={
                        "user_affected": str(user.id),
                        "action": "grant",
                        "permission": "admin",
                    },
                )
            for user in dataset.aggregated_users.all():
                Activity.objects.create(
                    type="dataset permission",
                    dataset=dataset,
                    user=real_user,
                    meta={
                        "user_affected": str(user.id),
                        "action": "grant",
                        "permission": "aggregated_access",
                    },
                )
            for user in dataset.full_access_users.all():
                Activity.objects.create(
                    type="dataset permission",
                    dataset=dataset,
                    user=real_user,
                    meta={
                        "user_affected": str(user.id),
                        "action": "grant",
                        "permission": "full_access",
                    },
                )

            return Response(data, status=201)
        else:
            return BadRequestErrorResponse(f"Bad Request: {dataset_serialized.errors}")

    def update(self, request, *args, **kwargs):
        dataset_serialized = self.serializer_class(data=request.data, allow_null=True)

        if dataset_serialized.is_valid():
            dataset_data = dataset_serialized.validated_data

            error_response = self.logic_validate(request, dataset_data)
            if error_response:
                return error_response

            dataset = self.get_object()

            if request.user.permission(dataset) != "admin":
                return ForbiddenErrorResponse(
                    f"This user can't update the following dataset {dataset.name}"
                    f"with following id {dataset.id}"
                )

            # activity
            updated_admin = set(dataset_data["admin_users"])
            existing = set(dataset.admin_users.all())
            diff = updated_admin ^ existing
            new = diff & updated_admin
            removed_admins = diff & existing

            if new:
                handle_event(
                    MonitorEvents.EVENT_DATASET_ADD_USER,
                    {
                        "dataset": dataset,
                        "view_request": request,
                        "additional_data": {
                            "user_list": [user.display_name for user in new],
                            "permission": "admin",
                        },
                    },
                )

            if removed_admins:
                handle_event(
                    MonitorEvents.EVENT_DATASET_REMOVE_USER,
                    {
                        "dataset": dataset,
                        "view_request": request,
                        "additional_data": {
                            "user_list": [user.display_name for user in removed_admins],
                            "permission": "admin",
                        },
                    },
                )

            for user in new:
                Activity.objects.create(
                    type="dataset permission",
                    dataset=dataset,
                    user=request.user,
                    meta={
                        "user_affected": str(user.id),
                        "action": "grant",
                        "permission": "admin",
                    },
                )
            for user in removed_admins:
                Activity.objects.create(
                    type="dataset permission",
                    dataset=dataset,
                    user=request.user,
                    meta={
                        "user_affected": str(user.id),
                        "action": "remove",
                        "permission": "admin",
                    },
                )

            updated_agg = set(dataset_data["aggregated_users"])
            existing = set(dataset.aggregated_users.all())
            diff = updated_agg ^ existing
            new = diff & updated_agg
            removed_agg = diff & existing

            if new:
                handle_event(
                    MonitorEvents.EVENT_DATASET_ADD_USER,
                    {
                        "dataset": dataset,
                        "view_request": request,
                        "additional_data": {
                            "user_list": [user.display_name for user in new],
                            "permission": "aggregated",
                        },
                    },
                )

            if removed_agg:
                handle_event(
                    MonitorEvents.EVENT_DATASET_REMOVE_USER,
                    {
                        "dataset": dataset,
                        "view_request": request,
                        "additional_data": {
                            "user_list": [user.display_name for user in removed_agg],
                            "permission": "aggregated",
                        },
                    },
                )

            for user in new:
                Activity.objects.create(
                    type="dataset permission",
                    dataset=dataset,
                    user=request.user,
                    meta={
                        "user_affected": str(user.id),
                        "action": "grant",
                        "permission": "aggregated_access",
                    },
                )

            updated_full = set(dataset_data["full_access_users"])
            existing = set(dataset.full_access_users.all())
            diff = updated_full ^ existing
            new = diff & updated_full
            removed_full = diff & existing

            if new:
                handle_event(
                    MonitorEvents.EVENT_DATASET_ADD_USER,
                    {
                        "dataset": dataset,
                        "view_request": request,
                        "additional_data": {
                            "user_list": [user.display_name for user in new],
                            "permission": "full_access",
                        },
                    },
                )

            if removed_full:
                handle_event(
                    MonitorEvents.EVENT_DATASET_REMOVE_USER,
                    {
                        "dataset": dataset,
                        "view_request": request,
                        "additional_data": {
                            "user_list": [user.display_name for user in removed_full],
                            "permission": "full_access",
                        },
                    },
                )

            for user in new:
                Activity.objects.create(
                    type="dataset permission",
                    dataset=dataset,
                    user=request.user,
                    meta={
                        "user_affected": str(user.id),
                        "action": "grant",
                        "permission": "full_access",
                    },
                )

            all_removed_users = removed_admins | removed_agg | removed_full
            for user in all_removed_users:
                if user not in (updated_admin | updated_agg | updated_full):
                    Activity.objects.create(
                        type="dataset permission",
                        dataset=dataset,
                        user=request.user,
                        meta={
                            "user_affected": str(user.id),
                            "action": "remove",
                            "permission": "all",
                        },
                    )
            if dataset.cover != request.data["cover"]:
                if not request.data["cover"].lower().startswith("dataset/gallery"):
                    file_name = request.data["cover"]
                    workdir = "/tmp/"
                    s3_client = aws_service.create_s3_client(
                        org_name=settings.LYNX_ORGANIZATION
                    )
                    local_path = os.path.join(workdir, file_name)
                    try:
                        lib.validate_file_type(
                            s3_client=s3_client,
                            bucket=settings.LYNX_FRONT_STATIC_BUCKET,
                            workdir="/tmp/dataset/",
                            object_key=file_name,
                            local_path=local_path,
                            file_types=self.file_types,
                        )
                    except Exception as e:
                        return BadRequestErrorResponse(error=e)

        result = super(self.__class__, self).update(
            request=self.request
        )  # will handle the case where serializer is not valid

        updated_dataset = self.get_object()
        process_structured_data_sources_in_background(dataset=updated_dataset)

        return result

    def destroy(self, request, *args, **kwargs):
        dataset = self.get_object()
        if request.user.permission(dataset) != "admin":
            return ForbiddenErrorResponse(
                f"This user can't delete the following dataset {dataset.name}"
                f"with following id {dataset.id}"
            )

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

        dataset.delete()
        return Response(status=204)
        # return super(self.__class__, self).destroy(request=self.request)
