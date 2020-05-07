import json
import logging
import os
import shutil
import threading
import time
import uuid

import boto3
import botocore.exceptions
import dateparser
import pyreadstat
from django.core import exceptions
from django.db import transaction
from django.db.utils import IntegrityError
from rest_framework.decorators import action
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.viewsets import ModelViewSet, ReadOnlyModelViewSet
from rest_framework_swagger.views import get_swagger_view

# noinspection PyPackageRequirements
from slugify import slugify

from mainapp import resources, settings
from mainapp.exceptions import (
    UnableToGetGlueColumns,
    UnsupportedColumnTypeError,
    QueryExecutionError,
    InvalidExecutionId,
    MaxExecutionReactedError,
    BucketNotFound,
)
from mainapp.models import (
    User,
    Organization,
    Study,
    Dataset,
    DataSource,
    Tag,
    Execution,
    Activity,
    Request,
    Documentation,
    StudyDataset,
)
from mainapp.serializers import (
    UserSerializer,
    OrganizationSerializer,
    DocumentationSerializer,
    TagSerializer,
    DataSourceSerializer,
    ActivitySerializer,
    RequestSerializer,
    DatasetSerializer,
    StudySerializer,
    SimpleQuerySerializer,
    QuerySerializer,
    CohortSerializer,
)
from mainapp.utils import devexpress_filtering
from mainapp.utils import statistics, lib, aws_service
from mainapp.utils.response_handler import (
    ErrorResponse,
    ForbiddenErrorResponse,
    NotFoundErrorResponse,
    ConflictErrorResponse,
    BadRequestErrorResponse,
    UnimplementedErrorResponse,
)

schema_view = get_swagger_view(title="Lynx API")
logger = logging.getLogger(__name__)


class GetSTS(APIView):  # from execution
    # noinspection PyMethodMayBeStatic
    def get(self, request):

        execution = request.user.the_execution.last()
        # service = request.query_params.get('service')

        try:
            study = Study.objects.filter(execution=execution).last()
        except Study.DoesNotExist:
            return ErrorResponse("This is not the execution of any study")

        # Create IAM client
        # request user is the execution user
        sts_default_provider_chain = aws_service.create_sts_client(
            org_name=request.user.organization.name
        )

        workspace_bucket_name = study.bucket

        org_name = request.user.organization.name

        role_to_assume_arn = f"arn:aws:iam::{settings.ORG_VALUES[org_name]['ACCOUNT_NUMBER']}:role/{workspace_bucket_name}"

        try:
            response = sts_default_provider_chain.assume_role(
                RoleArn=role_to_assume_arn, RoleSessionName="session"
            )
        except botocore.exceptions.ClientError as e:
            error = Exception(
                f"Error calling 'assume_role' in 'GetSTS' for study {study.id}, organization: {org_name}"
            ).with_traceback(e.__traceback__)
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=error,
            )
        except Exception as e:
            error = Exception(
                f"There was an error when requesting STS credentials for study {study.id}"
            ).with_traceback(e.__traceback__)
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=error,
            )

        config = {
            "bucket": workspace_bucket_name,
            "aws_sts_creds": response["Credentials"],
            "region": settings.ORG_VALUES[org_name]["AWS_REGION"],
        }

        return Response(config)


class GetStaticSTS(APIView):  # from execution
    # noinspection PyMethodMayBeStatic
    def get(self, request):
        sts_default_provider_chain = aws_service.create_sts_client()
        static_bucket_name = settings.LYNX_FRONT_STATIC_BUCKET
        role_to_assume_arn = (
            f"arn:aws:iam::{settings.ORG_VALUES['Lynx MD']['ACCOUNT_NUMBER']}:role/"
            f"{settings.AWS_STATIC_ROLE_NAME}"
        )

        try:
            response = sts_default_provider_chain.assume_role(
                RoleArn=role_to_assume_arn, RoleSessionName="session"
            )
        except botocore.exceptions.ClientError as e:
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=e,
            )
        except Exception as e:
            error = Exception(
                f"The server can't process your request due to unexpected internal error"
            ).with_traceback(e.__traceback__)
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=error,
            )

        config = {
            "bucket": static_bucket_name,
            "aws_sts_creds": response["Credentials"],
        }

        return Response(config)


class Dummy(APIView):  # usage in Lambda Function
    # noinspection PyMethodMayBeStatic
    def get(self, request):
        return Response()


class GetExecution(APIView):  # from frontend
    @transaction.atomic
    # noinspection PyMethodMayBeStatic
    def get(self, request):
        study_id = request.query_params.get("study")

        try:
            study = request.user.studies.get(id=study_id)
        except Study.DoesNotExist:
            return NotFoundErrorResponse(f"Study {study_id} does not exists")

        if request.user not in study.users.all():
            return ForbiddenErrorResponse(
                f"Only users that have this study {study_id} can get a study execution"
            )

        if not study.execution:
            execution_id = uuid.uuid4()

            # headers = {
            # "Authorization": "Bearer " + settings['JH_API_ADMIN_TOKEN'], "ALBTOKEN": settings['JH_ALB_TOKEN']
            # }
            #
            # data = {
            #     "usernames": [
            #         str(id).split("-")[-1]
            #     ],
            #     "admin": False
            # }
            # res = requests.post(settings.jh_url + "hub/api/users", json=data, headers=headers, verify=False)
            # if res.status_code != 201:
            #     return Error(
            #     "error creating a user for the execution in JH: " + str(res.status_code) + ", " + res.text
            #     )

            # execution.study = study
            execution = Execution.objects.create(id=execution_id)
            execution.real_user = request.user
            execution_user = User.objects.create_user(
                email=execution.token + "@lynx.md"
            )
            execution_user.set_password(execution.token)
            execution_user.organization = study.datasets.first().organization
            execution_user.is_execution = True
            execution_user.save()
            execution.execution_user = execution_user
            execution.save()
            study.execution = execution
            study.save()

        return Response({"execution_identifier": str(study.execution.token)})


class StudyViewSet(ModelViewSet):
    http_method_names = ["get", "head", "post", "put", "delete"]
    filter_fields = ("user_created",)

    serializer_class = StudySerializer

    @action(detail=True, methods=["get"])
    def get_study_per_organization(self, request, pk=None):
        study = self.get_object()
        dataset = study.datasets.first()
        if not dataset:
            return UnimplementedErrorResponse(
                f"Bad Request: Study {study} does not have datasets"
            )
        organization_name = dataset.organization.name
        return Response({"study_organization": organization_name})

    def get_queryset(self, **kwargs):
        return self.request.user.related_studies.all()

    def create(self, request, **kwargs):
        study_serialized = self.serializer_class(data=request.data)
        if study_serialized.is_valid():

            req_datasets = study_serialized.validated_data["studydataset_set"]
            study_name = study_serialized.validated_data["name"]

            # TODO need to decide what to do with repeated datasets names:
            # TODO for example - if user A shared a dataset with user B ant the former has a dataset with the same name
            # if study_name in [x.name for x in request.user.studies.all()]:
            #     return Error("this study already exist for that user")

            if not all(
                rds["dataset"] in request.user.datasets.all() for rds in req_datasets
            ):
                return ForbiddenErrorResponse(
                    f"Not all datasets are related to the current user {request.user.id}"
                )

            study = Study.objects.create(name=study_name)
            study.description = study_serialized.validated_data["description"]
            req_users = study_serialized.validated_data["users"]

            study_datasets = map(
                lambda x: StudyDataset.objects.create(study=study, **x), req_datasets
            )
            study.studydataset_set.set(study_datasets)
            study.users.set(
                [request.user]
                + list(User.objects.filter(id__in=[x.id for x in req_users]))
            )  # can user add also..
            study.user_created = request.user

            req_tags = study_serialized.validated_data["tags"]
            study.tags.set(Tag.objects.filter(id__in=[x.id for x in req_tags]))

            study.save()

            workspace_bucket_name = study.bucket
            org_name = study.organization
            s3 = aws_service.create_s3_client(org_name=org_name)

            try:
                lib.create_s3_bucket(
                    org_name=org_name, name=workspace_bucket_name, s3_client=s3
                )
            except botocore.exceptions.ClientError as e:
                error = Exception(
                    f"The server can't process your request due to unexpected internal error"
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )
            except Exception as e:
                error = Exception(
                    f"There was an error when trying to create a bucket for workspace: {study.id}"
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )

            time.sleep(1)  # wait for the bucket to be created

            policy_json = {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": []}],
            }

            policy_json["Statement"][0]["Resource"].append(
                "arn:aws:s3:::" + workspace_bucket_name + "*"
            )

            for dataset in study.datasets.all():
                policy_json["Statement"][0]["Resource"].append(
                    "arn:aws:s3:::" + dataset.bucket + "*"
                )

            client = aws_service.create_iam_client(org_name=org_name)

            try:
                response = client.create_policy(
                    PolicyName=workspace_bucket_name,
                    PolicyDocument=json.dumps(policy_json),
                )
            except botocore.exceptions.ClientError as e:
                error = Exception(
                    f"The server can't process your request due to unexpected internal error"
                ).with_traceback(e.__traceback__)
                return ForbiddenErrorResponse(
                    f"Unauthorized to perform this request", error=error
                )
            except Exception as e:
                error = Exception(
                    f"The server can't process your request due to unexpected internal error for study workspace."
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )

            policy_arn = response["Policy"]["Arn"]

            trust_policy_json = resources.create_base_trust_relationship(
                org_name=org_name
            )
            try:
                client.create_role(
                    RoleName=workspace_bucket_name,
                    AssumeRolePolicyDocument=json.dumps(trust_policy_json),
                    Description=workspace_bucket_name,
                )
            except botocore.exceptions.ClientError as e:
                error = Exception(
                    f"The server can't process your request due to unexpected internal error."
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )
            except Exception as e:
                error = Exception(
                    f"There was an error creating the role: {workspace_bucket_name}"
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )

            try:
                client.attach_role_policy(
                    RoleName=workspace_bucket_name, PolicyArn=policy_arn
                )
            except botocore.exceptions.ClientError as e:
                error = Exception(
                    f"The server can't process your request due to unexpected internal error."
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )
            except Exception as e:
                error = Exception(
                    f"The server can't process your request due to unexpected internal error."
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )

            for dataset in study.datasets.all():
                Activity.objects.create(
                    dataset=dataset,
                    study=study,
                    user=request.user,
                    type="dataset assignment",
                )

            return Response(
                self.serializer_class(study, allow_null=True).data, status=201
            )
        else:
            return ErrorResponse(study_serialized.errors)

    def update(self, request, *args, **kwargs):
        serialized = self.serializer_class(data=request.data, allow_null=True)

        if serialized.is_valid():  # if not valid super will handle it
            study_updated = serialized.validated_data

            study = self.get_object()
            if request.user not in study.users.all():
                return ForbiddenErrorResponse(
                    f"Only the study creator can edit a study"
                )

            org_name = study.organization

            client = aws_service.create_iam_client(org_name=org_name)
            account_number = settings.ORG_VALUES["Lynx MD"]["ACCOUNT_NUMBER"]
            policy_arn = (
                f"arn:aws:iam::{account_number}:policy/lynx-workspace-{study.id}"
            )

            role_name = f"lynx-workspace-{study.id}"

            try:
                client.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
                client.delete_policy(PolicyArn=policy_arn)
            except client.exceptions.NoSuchEntityException:
                logger.warning(
                    f"Ignoring detaching and deleting role policy that not exist for role-name: {role_name}"
                )
            except Exception as e:
                error = Exception(
                    f"There was an unexpected error while detaching and deleting the role: {role_name}"
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )

            policy_json = {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": []}],
            }

            policy_name = f"lynx-workspace-{study.id}"
            workspace_bucket_name = f"lynx-workspace-{study.id}"
            policy_json["Statement"][0]["Resource"].append(
                f"arn:aws:s3:::{workspace_bucket_name}*"
            )

            datasets = study_updated["studydataset_set"]
            for dataset_data in datasets:
                policy_json["Statement"][0]["Resource"].append(
                    f'arn:aws:s3:::{dataset_data["dataset"].bucket}*'
                )

            try:
                response = client.create_policy(
                    PolicyName=policy_name, PolicyDocument=json.dumps(policy_json)
                )
            except botocore.exceptions.ClientError as e:
                error = Exception(
                    f"The server can't process your request due to unexpected internal error."
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )
            except Exception as e:
                error = Exception(
                    f"There was an error creating this policy: {policy_name}"
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Unexpected error. Server was not able to complete this request.",
                    error=error,
                )
            policy_arn = response["Policy"]["Arn"]

            role_name = f"lynx-workspace-{study.id}"

            client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)

            updated_datasets = set(map(lambda x: x["dataset"], datasets))
            existing_datasets = set(study.datasets.all())
            diff_datasets = updated_datasets ^ existing_datasets
            for d in diff_datasets & updated_datasets:
                Activity.objects.create(
                    type="dataset assignment", study=study, dataset=d, user=request.user
                )

        return super(self.__class__, self).update(request=self.request)


class GetDatasetSTS(APIView):  # for frontend uploads
    # noinspection PyMethodMayBeStatic
    def get(self, request, dataset_id):
        try:
            dataset = request.user.datasets.get(id=dataset_id)
        except Dataset.DoesNotExist:
            return NotFoundErrorResponse(
                f"Dataset with that dataset_id {dataset_id} does not exists"
            )

        # generate sts token so the user can upload the dataset to the bucket
        org_name = dataset.organization.name
        sts_default_provider_chain = aws_service.create_sts_client(org_name=org_name)
        # sts_default_provider_chain = aws_service.create_sts_client()

        role_name = f"lynx-dataset-{dataset.id}"
        role_to_assume_arn = (
            f"arn:aws:iam::{settings.ORG_VALUES[org_name]['ACCOUNT_NUMBER']}"
            f":role/{role_name}"
        )

        try:
            sts_response = sts_default_provider_chain.assume_role(
                RoleArn=role_to_assume_arn,
                RoleSessionName="session",
                DurationSeconds=43200,
            )
        except botocore.exceptions.ClientError as e:
            error = Exception(
                f"The server can't process your request due to unexpected internal error"
            ).with_traceback(e.__traceback__)
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=error,
            )
        except Exception as e:
            error = Exception(
                f"There was an error creating STS token for dataset: {dataset_id}"
            ).with_traceback(e.__traceback__)
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=error,
            )

        config = {
            "bucket": dataset.bucket,
            "aws_sts_creds": sts_response["Credentials"],
            "region": settings.ORG_VALUES[org_name]["AWS_REGION"],
        }
        return Response(config)


class GetStudySTS(APIView):  # for frontend uploads
    # noinspection PyMethodMayBeStatic
    def get(self, request, study_id):
        try:
            study = request.user.studies.get(id=study_id)
        except Dataset.DoesNotExist as e:
            raise NotFoundErrorResponse(
                f"Study with that id {study_id} does not exists"
            ) from e

        # generate sts token so the user can upload the dataset to the bucket
        org_name = study.organization

        sts_default_provider_chain = aws_service.create_sts_client(org_name=org_name)

        role_name = f"lynx-workspace-{study.id}"
        role_to_assume_arn = f"arn:aws:iam::{settings.ORG_VALUES[org_name]['ACCOUNT_NUMBER']}:role/{role_name}"

        try:
            sts_response = sts_default_provider_chain.assume_role(
                RoleArn=role_to_assume_arn,
                RoleSessionName="session",
                DurationSeconds=43200,
            )
        except botocore.exceptions.ClientError as e:
            error = Exception(
                f"The server can't process your request due to unexpected internal error"
            ).with_traceback(e.__traceback__)
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=error,
            )
        except Exception as e:
            error = Exception(
                f"There was an error creating a STS token for this study: {study.id}"
            ).with_traceback(e.__traceback__)
            return ErrorResponse(
                f"Unexpected error. Server was not able to complete this request.",
                error=error,
            )
        config = {
            "bucket": study.bucket,
            "aws_sts_creds": sts_response["Credentials"],
            "region": settings.ORG_VALUES[org_name]["AWS_REGION"],
        }

        return Response(config)


class HandleDatasetAccessRequest(APIView):
    def get(self, request, user_request_id):
        possible_responses = ["approve", "deny"]
        response = request.query_params.get("response")

        if response not in possible_responses:
            return NotFoundErrorResponse(
                f"Please response with query string param: {possible_responses}"
            )

        try:
            user_request = self.request.user.requests_for_me.get(id=user_request_id)
        except Request.DoesNotExist:
            return NotFoundErrorResponse("Request not found")

        user_request.state = "approved" if response is "approve" else "denied"
        user_request.save()

        return Response()


class RequestViewSet(ModelViewSet):
    http_method_names = ["get", "head", "post"]
    serializer_class = RequestSerializer
    filter_fields = (
        "user_requested",
        "dataset",
        "study",
        "type",
        "state",
        "permission",
    )

    def get_queryset(self):
        return self.request.user.requests_for_me

    def create(self, request, **kwargs):
        request_serialized = self.serializer_class(data=request.data, allow_null=True)

        if request_serialized.is_valid():
            request_data = request_serialized.validated_data

            if request_data["type"] == "dataset_access":
                permission_request_types = ["aggregated_access", "full_access"]

                if "dataset" not in request_data:
                    return NotFoundErrorResponse(
                        "Please mention dataset if type is dataset_access"
                    )

                if request_data["dataset"] not in request.user.datasets.filter(
                    state="private"
                ):
                    return NotFoundErrorResponse(
                        f"Can not request access for a dataset that is not private"
                    )

                dataset = request_data["dataset"]

                if "permission" not in request_data:
                    return NotFoundErrorResponse(
                        "Please mention a permission for that kind of request"
                    )

                if request_data["permission"] not in permission_request_types:
                    return NotFoundErrorResponse(
                        f"Permission must be one of: {permission_request_types}"
                    )

                # the logic validations:
                if (
                    request.user.permission(dataset) == "full_access"
                    and request_data["permission"] == "full_access"
                ):
                    return ConflictErrorResponse(
                        f"You already have {request_data['permission']} access for this dataset {dataset.name}"
                        f"with following dataset id {dataset.id}"
                    )

                if (
                    request.user.permission(dataset) == "full_access"
                    and request_data["permission"] == "aggregated_access"
                ):
                    return ConflictErrorResponse(
                        f"You already have aggregated access for this dataset {dataset.name}"
                        f"with following dataset id{dataset.id}"
                    )

                if request.user.permission(dataset) is "admin":
                    return ConflictErrorResponse(
                        f"You are already an admin of this dataset {dataset.name} "
                        f"with the following dataset id {dataset.id}. "
                        f"Your are granted with full permission"
                    )

                existing_requests = Request.objects.filter(
                    dataset=dataset,
                    type="dataset_access",
                    user_requested=request.user,
                    state="pending",
                )

                if existing_requests.filter(permission="aggregated_access"):
                    if request_data["permission"] == "aggregated_access":
                        return ConflictErrorResponse(
                            f"You already requested aggregated access for this dataset {dataset.name}"
                            f"with following dataset id {dataset.id}"
                        )
                    if request_data["permission"] == "full_access":
                        return ConflictErrorResponse(
                            f"You have already requested aggregated access for this dataset {dataset.name} "
                            f"with the following dataset id {dataset.id}."
                            "You have to wait for an admin to response your current request "
                            "before requesting full access"
                        )

                if existing_requests.filter(permission="full_access"):
                    return ConflictErrorResponse(
                        f"You have already requested full access for that dataset {dataset.name}"
                    )

                request_data["user_requested"] = request.user
                request = request_serialized.save()

                return Response(
                    self.serializer_class(request, allow_null=True).data, status=201
                )
        else:
            return BadRequestErrorResponse(
                f"Unknown request data type {request_serialized.errors}"
            )


class MyRequestsViewSet(ReadOnlyModelViewSet):
    filter_fields = ("dataset", "study", "type", "state", "permission")
    serializer_class = RequestSerializer

    def get_queryset(self):
        return self.request.user.my_requests


class AWSHealthCheck(APIView):
    authentication_classes = []
    permission_classes = []

    # noinspection PyMethodMayBeStatic
    def get(self, request):
        return Response()


class CurrentUserView(APIView):
    # noinspection PyMethodMayBeStatic
    def get(self, request):
        serializer = UserSerializer(request.user)
        return Response(serializer.data)


class UserViewSet(ReadOnlyModelViewSet):
    serializer_class = UserSerializer
    queryset = User.objects.filter(is_execution=False)


class OrganizationViewSet(ReadOnlyModelViewSet):
    serializer_class = OrganizationSerializer

    def get_queryset(self, **kwargs):
        return Organization.objects.all()


class TagViewSet(ReadOnlyModelViewSet):
    serializer_class = TagSerializer
    queryset = Tag.objects.all()


class DatasetViewSet(ModelViewSet):
    http_method_names = ["get", "head", "post", "put", "delete"]
    serializer_class = DatasetSerializer
    filter_fields = ("ancestor",)

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

    def get_queryset(self):
        return self.request.user.datasets

    def create(self, request, **kwargs):
        dataset_serialized = self.serializer_class(data=request.data, allow_null=True)

        if dataset_serialized.is_valid():
            # create the dataset insance:
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
            )
            dataset.id = uuid.uuid4()

            # aws stuff
            org_name = request.user.organization.name
            s3 = aws_service.create_s3_client(org_name=org_name)

            try:
                lib.create_s3_bucket(
                    org_name=org_name, name=dataset.bucket, s3_client=s3
                )
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
            dataset.state = dataset_data["state"]
            dataset.default_user_permission = dataset_data["default_user_permission"]
            req_tags = dataset_data["tags"]
            dataset.tags.set(Tag.objects.filter(id__in=[x.id for x in req_tags]))
            dataset.user_created = request.user
            dataset.ancestor = (
                dataset_data["ancestor"] if "ancestor" in dataset_data else None
            )
            if "ancestor" in dataset_data:
                dataset.is_discoverable = False
                dataset.state = "private"
                if dataset.default_user_permission == "aggregated_access":
                    dataset.aggregated_users.add(request.user.id)
            dataset.organization = (
                dataset.ancestor.organization
                if dataset.ancestor
                else request.user.organization
            )
            # dataset.bucket = 'lynx-dataset-' + str(dataset.id)
            dataset.programmatic_name = (
                slugify(dataset.name) + "-" + str(dataset.id).split("-")[0]
            )

            dataset.save()

            # the role takes this time to be created!
            # it is here in order to prevent calling GetDatasetSTS before creation
            time.sleep(8)

            data = self.serializer_class(dataset, allow_null=True).data

            # activity:
            for user in dataset.admin_users.all():
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
            for user in dataset.aggregated_users.all():
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
            for user in dataset.full_access_users.all():
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
            # for user in removed_agg:
            #     Activity.objects.create(type="dataset remove permission", dataset=dataset, user=request.user,
            #                             meta={"user_affected":  str(user.id),"permission":"aggregated"})

            updated_full = set(dataset_data["full_access_users"])
            existing = set(dataset.full_access_users.all())
            diff = updated_full ^ existing
            new = diff & updated_full
            removed_full = diff & existing

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
            # for user in removed_full:
            #     Activity.objects.create(type="dataset remove permission", dataset=dataset, user=request.user,
            #                             meta={"user_affected": str(user.id), "permission": "full"})

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

        return super(self.__class__, self).update(
            request=self.request
        )  # will handle the case where serializer is not valid

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
            logger.info(f"All subsets were deleted for dataset {dataset_to_delete.id}")

        delete_tree_raw = request.GET.get("delete_tree")
        delete_tree = True if delete_tree_raw == "true" else False

        if delete_tree:
            delete_dataset_tree(dataset)

        elif dataset.ancestor:
            for child in dataset.children.all():
                child.ancestor = dataset.ancestor
                child.save()

        dataset.delete()
        logger.info(f"Dataset root was deleted {dataset.id}")
        return Response(status=204)
        # return super(self.__class__, self).destroy(request=self.request)


class DataSourceViewSet(ModelViewSet):
    serializer_class = DataSourceSerializer
    http_method_names = ["get", "head", "post", "put", "delete"]
    filter_fields = ("dataset",)

    @action(detail=True, methods=["get"])
    def statistics(self, request, *args, **kwargs):
        data_source = self.get_object()
        if data_source.state != "ready":
            return ErrorResponse(
                f"Data is still in processing. Datasource id {data_source.name}",
                status_code=503,
            )

        glue_database = data_source.dataset.glue_database
        bucket_name = f"lynx-{glue_database}"
        glue_table = data_source.glue_table
        query_from_front = request.query_params.get("grid_filter")
        if query_from_front:
            query_from_front = json.loads(query_from_front)

        org_name = data_source.dataset.organization.name
        try:
            columns_types = lib.get_columns_types(
                org_name=org_name, glue_database=glue_database, glue_table=glue_table
            )
            default_athena_col_names = statistics.create_default_column_names(
                columns_types
            )
        except UnableToGetGlueColumns as e:
            return ErrorResponse(f"Glue error", error=e)

        try:
            filter_query = (
                None
                if not query_from_front
                else devexpress_filtering.generate_where_sql_query(query_from_front)
            )
            query = statistics.sql_builder_by_columns_types(
                glue_table, columns_types, default_athena_col_names, filter_query
            )
        except UnsupportedColumnTypeError as e:
            return UnimplementedErrorResponse(
                "There was some error in execution", error=e
            )
        except Exception as e:
            return ErrorResponse("There was some error in execution", error=e)

        try:
            response = statistics.count_all_values_query(
                query, glue_database, bucket_name, org_name
            )
            data_per_column = statistics.sql_response_processing(
                response, default_athena_col_names
            )
            final_result = {"result": data_per_column, "columns_types": columns_types}
        except QueryExecutionError as e:
            return ErrorResponse(
                "There was some error in execution", error=e, status_code=502
            )
        except (InvalidExecutionId, MaxExecutionReactedError) as e:
            return ErrorResponse("There was some error in execution", error=e)
        except KeyError as e:
            return ErrorResponse(
                "Unexpected error: invalid or missing query result set", error=e
            )
        except Exception as e:
            return ErrorResponse("There was some error in execution", error=e)

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
        ds_types = ["structured", "images", "zip"]
        data_source_serialized = self.serializer_class(
            data=request.data, allow_null=True
        )

        if data_source_serialized.is_valid():
            data_source_data = data_source_serialized.validated_data
            dataset = data_source_data["dataset"]

            if dataset not in request.user.datasets.all():
                return BadRequestErrorResponse(
                    f"Dataset {dataset.id} does not exist or does not belong to the user"
                )

            if data_source_data["type"] not in ds_types:
                return BadRequestErrorResponse(
                    f"Data source type must be one of: {ds_types}"
                )

            if "s3_objects" in data_source_data:
                if not isinstance(data_source_data["s3_objects"], list):
                    return ForbiddenErrorResponse("s3 objects must be a (json) list")

            if data_source_data["type"] in ["zip", "structured"]:
                if "s3_objects" not in data_source_data:
                    logger.exception("s3_objects field must be included")

                if len(data_source_data["s3_objects"]) != 1:
                    return BadRequestErrorResponse(
                        f"Data source of type {data_source_data['type']} "
                        f"structured and zip must include exactly one item in s3_objects json array"
                    )

                s3_obj = data_source_data["s3_objects"][0]["key"]
                path, file_name, file_name_no_ext, ext = lib.break_s3_object(s3_obj)
                if ext not in ["sav", "zsav", "csv"]:
                    return BadRequestErrorResponse(
                        "File type is not supported as a structured data source"
                    )

            data_source = data_source_serialized.save()
            data_source.programmatic_name = (
                slugify(data_source.name) + "-" + str(data_source.id).split("-")[0]
            )
            data_source.save()

            if data_source.type == "structured":
                s3_obj = data_source.s3_objects[0]["key"]
                path, file_name, file_name_no_ext, ext = lib.break_s3_object(s3_obj)

                if ext in ["sav", "zsav"]:  # convert to csv
                    s3_client = aws_service.create_s3_client(
                        org_name=data_source.dataset.organization.name
                    )
                    workdir = f"/tmp/{data_source.id}"
                    os.makedirs(workdir)
                    try:
                        s3_client.download_file(
                            data_source.dataset.bucket,
                            s3_obj,
                            workdir + "/" + file_name,
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
                        s3_client.upload_file(
                            csv_path_and_file,
                            data_source.dataset.bucket,
                            path + "/" + file_name_no_ext + ".csv",
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

                data_source.state = "pending"
                data_source.save()
                create_catalog_thread = threading.Thread(
                    target=lib.create_catalog,
                    kwargs={
                        "org_name": request.user.organization.name,
                        "data_source": data_source,
                    },
                )  # also setting the data_source state to ready when it's done
                create_catalog_thread.start()

            elif data_source.type == "zip":
                data_source.state = "pending"
                data_source.save()
                handle_zip_thread = threading.Thread(
                    target=lib.handle_zipped_data_source,
                    args=[data_source, request.user.organization.name],
                )
                handle_zip_thread.start()

            else:
                data_source.state = "ready"

            data_source.save()
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


class RunQuery(GenericAPIView):
    serializer_class = SimpleQuerySerializer

    def post(self, request):
        query_serialized = self.serializer_class(data=request.data)
        if query_serialized.is_valid():
            execution = request.user.the_execution.last()

            try:
                study = Study.objects.get(execution=execution)
            except Study.DoesNotExist:
                return ErrorResponse("This is not the execution of any study")

            req_dataset_id = query_serialized.validated_data["dataset_id"]

            try:
                dataset = study.datasets.get(id=req_dataset_id)
            except Dataset.DoesNotExist as e:
                return ForbiddenErrorResponse(
                    f"No permission to this dataset. "
                    f"Make sure it exists, it's yours or shared with you, and under that study",
                    error=e,
                )

            query_string = query_serialized.validated_data["query_string"]

            access = lib.calc_access_to_database(execution.real_user, dataset)

            if access == "aggregated access":
                if not lib.is_aggregated(query_string):
                    return ErrorResponse(
                        "This is not an aggregated query. Only aggregated queries are allowed"
                    )

            if access == "no access":
                return ForbiddenErrorResponse(f"No permission to query this dataset")

            org_name = dataset.organization.name
            client = aws_service.create_athena_client(org_name=org_name)
            try:
                response = client.start_query_execution(
                    QueryString=query_string,
                    QueryExecutionContext={
                        "Database": dataset.glue_database  # the name of the database in glue/athena
                    },
                    ResultConfiguration={
                        "OutputLocation": f"s3://lynx-workspace-{study.id}/temp_execution_results"
                    },
                )
            except Exception as e:
                error = Exception(
                    f"Failed to start_query_execution with the following error"
                ).with_traceback(e.__traceback__)
                return ErrorResponse(
                    f"Could not execute the query for dataset {dataset.id}", error=error
                )
            Activity.objects.create(
                user=execution.real_user,
                dataset=dataset,
                study=study,
                meta={"query_string": query_string},
                type="query",
            )
            return Response({"query_execution_id": response["QueryExecutionId"]})
        else:
            return query_serialized.errors


class CreateCohort(GenericAPIView):
    serializer_class = CohortSerializer

    def post(self, request):
        query_serialized = self.serializer_class(data=request.data)
        if query_serialized.is_valid():

            user = request.user
            req_dataset_id = query_serialized.validated_data["dataset_id"]

            try:
                dataset = user.datasets.get(id=req_dataset_id)
            except Dataset.DoesNotExist as e:
                return ForbiddenErrorResponse(
                    f"No permission to this dataset. Make sure it exists, it's yours or shared with you",
                    error=e,
                )

            try:
                destination_dataset = user.datasets.get(
                    id=query_serialized.validated_data["destination_dataset_id"]
                )
            except Dataset.DoesNotExist as e:
                return ForbiddenErrorResponse(
                    f"Dataset not found or does not have permissions", error=e
                )

            try:
                data_source = dataset.data_sources.get(
                    id=query_serialized.validated_data["data_source_id"]
                )
            except DataSource.DoesNotExist as e:
                return ForbiddenErrorResponse(
                    f"Dataset not found or does not have permissions", error=e
                )

            access = lib.calc_access_to_database(user, dataset)

            if access == "no access":
                return ForbiddenErrorResponse(f"No permission to query this dataset")

            limit = query_serialized.validated_data["limit"]

            data_filter = (
                json.loads(query_serialized.validated_data["filter"])
                if "filter" in query_serialized.validated_data
                else None
            )
            columns = (
                json.loads(query_serialized.validated_data["columns"])
                if "columns" in query_serialized.validated_data
                else None
            )

            query, _ = devexpress_filtering.dev_express_to_sql(
                table=data_source.glue_table,
                schema=dataset.glue_database,
                data_filter=data_filter,
                columns=columns,
                limit=limit,
            )

            if not destination_dataset.glue_database:
                try:
                    lib.create_glue_database(
                        org_name=dataset.organization.name, dataset=destination_dataset
                    )
                except Exception as e:
                    error = Exception(
                        f"There was an error creating glue database: {dataset.glue_database}"
                    ).with_traceback(e.__traceback__)
                    return ErrorResponse(
                        f"Coud not create database for {dataset.glue_database}",
                        error=error,
                    )

            ctas_query = (
                'CREATE TABLE "'
                + destination_dataset.glue_database
                + '"."'
                + data_source.glue_table
                + '"'
                + " WITH (format = 'TEXTFILE', external_location = 's3://"
                + destination_dataset.bucket
                + "/"
                + data_source.glue_table
                + "/') AS "
                + query
                + ";"
            )

            logger.debug(f"Query result of CREATE TABLE AS SELECT {ctas_query}")

            client = aws_service.create_athena_client(
                org_name=dataset.organization.name
            )
            try:
                response = client.start_query_execution(
                    QueryString=ctas_query,
                    QueryExecutionContext={
                        "Database": dataset.glue_database  # the name of the database in glue/athena
                    },
                    ResultConfiguration={
                        "OutputLocation": f"s3://{destination_dataset.bucket}/temp_execution_results"
                    },
                )

            except client.exceptions.InvalidRequestException as e:
                error = Exception(
                    f"Failed executing the CTAS query: {ctas_query}. "
                    f"Query string: {query}"
                ).with_traceback(e.__traceback__)
                logger.debug(f"This is the ctas_query {ctas_query}")
                return ErrorResponse(
                    f"There was an error executing this query", error=error
                )

            logger.debug(f"Response of created query {response}")

            new_data_source = data_source
            new_data_source.id = None
            new_data_source.s3_objects = None
            new_data_source.dataset = destination_dataset
            cohort = {"filter": data_filter, "columns": columns, "limit": limit}
            new_data_source.cohort = cohort

            try:
                new_data_source.save()
            except IntegrityError as e:
                return ErrorResponse(
                    f"Dataset {dataset.id} already has datasource with same name {new_data_source}",
                    error=e,
                )

            new_data_source.ancestor = data_source
            new_data_source.save()

            req_res = {"query": query, "ctas_query": ctas_query}
            return Response(req_res, status=201)

        else:
            return BadRequestErrorResponse(
                "Bad Request:", error=query_serialized.errors
            )


class Query(GenericAPIView):
    serializer_class = QuerySerializer

    def post(self, request):
        query_serialized = self.serializer_class(data=request.data)

        if query_serialized.is_valid():
            user = request.user

            req_dataset_id = query_serialized.validated_data["dataset_id"]
            try:
                dataset = user.datasets.get(id=req_dataset_id)
            except Dataset.DoesNotExist as e:
                return NotFoundErrorResponse(
                    f"No permission to this dataset. Make sure it is exists, it's yours or shared with you",
                    error=e,
                )

            access = lib.calc_access_to_database(user, dataset)

            if access == "no access":
                return ForbiddenErrorResponse(f"No permission to query this dataset")

            # if access == "aggregated access":
            #    if not utils.is_aggregated(query_string):
            #        return Error("this is not an aggregated query. only aggregated queries are allowed")

            data_source_id = query_serialized.validated_data["data_source_id"]
            try:
                data_source = dataset.data_sources.get(id=data_source_id)
            except DataSource.DoesNotExist as e:
                return NotFoundErrorResponse(
                    f"Data source {data_source_id} for dataset {dataset.id} does not exists",
                    error=e,
                )

            if query_serialized.validated_data["query"]:

                query = query_serialized.validated_data["query"]
                query_no_limit, count_query, limit = lib.get_query_no_limit_and_count_query(
                    query
                )
                sample_aprx = None

            else:
                limit = query_serialized.validated_data["limit"]
                sample_aprx = query_serialized.validated_data["sample_aprx"]

                data_filter = (
                    json.loads(query_serialized.validated_data["filter"])
                    if "filter" in query_serialized.validated_data
                    else None
                )
                columns = (
                    json.loads(query_serialized.validated_data["columns"])
                    if "columns" in query_serialized.validated_data
                    else None
                )

                query, query_no_limit = devexpress_filtering.dev_express_to_sql(
                    table=data_source.glue_table,
                    data_filter=data_filter,
                    columns=columns,
                    limit=limit,
                )
                _, count_query, _ = lib.get_query_no_limit_and_count_query(query)

            req_res = {}

            final_query = query_no_limit

            return_count = True if request.GET.get("return_count") == "true" else False
            if sample_aprx or return_count:
                logger.debug(f"Count query: {count_query}")

                try:
                    response = dataset.query(count_query)
                except Exception as e:
                    return ErrorResponse(
                        f"Failed executing the query: {count_query} ."
                        f"Original query: {query}",
                        error=e,
                    )

                query_execution_id = response["QueryExecutionId"]

                try:
                    obj = dataset.get_query_execution(query_execution_id)
                except BucketNotFound as e:
                    error = Exception(
                        f"The requested bucket does not exist. Query result file was not found. Query string: {query}"
                    ).with_traceback(e.__traceback__)
                    return ErrorResponse(
                        f"Could not create result for following query {query}",
                        error=error,
                    )
                except Exception as e:
                    error = Exception(
                        f"Can not get s3 object, with following error"
                    ).with_traceback(e.__traceback__)
                    return ErrorResponse(
                        "Unknown error occurred during reading of the query result",
                        error=error,
                    )

                count = int(
                    obj["Body"].read().decode("utf-8").split("\n")[1].strip('"')
                )

                if return_count:
                    req_res["count_no_limit"] = count

                if sample_aprx:
                    if count > sample_aprx:
                        percentage = int((sample_aprx / count) * 100)
                        final_query = (
                            f"{query_no_limit} TABLESAMPLE BERNOULLI({percentage})"
                        )

            if limit:
                final_query += f" LIMIT {limit}"

            logger.debug(f"Final query: {final_query}")

            try:
                response = dataset.query(final_query)
            except Exception as e:
                error = Exception(
                    f"Failed to start_query_execution with the following error"
                ).with_traceback(e.__traceback__)
                logger.info(f"Final query {final_query}")
                return ErrorResponse(f"Query execution failed", error=error)

            req_res["query"] = final_query
            req_res["count_query"] = count_query
            query_execution_id = response["QueryExecutionId"]
            req_res["execution_result"] = {
                "query_execution_id": query_execution_id,
                "item": {
                    "bucket": dataset.bucket,
                    "key": f"temp_execution_results/{query_execution_id}.csv",
                },
            }

            return_result = (
                True if request.GET.get("return_result") == "true" else False
            )
            result_format = request.GET.get("result_format")

            if result_format and not return_result:
                return BadRequestErrorResponse(
                    "Why result_format and no return_result=true?"
                )

            return_columns_types = (
                True if request.GET.get("return_columns_types") == "true" else False
            )
            if return_columns_types or (return_result and result_format == "json"):
                columns_types = dataset.get_columns_types(
                    glue_table=data_source.glue_table
                )
                if return_columns_types:
                    req_res["columns_types"] = columns_types

            if return_result:
                try:
                    result_obj = dataset.get_query_execution(query_execution_id)
                except BucketNotFound as e:
                    error = Exception(
                        f"Query result file does not exist in bucket. Query string: {query}"
                    ).with_traceback(e.__traceback__)
                    logger.info(f"No result for query: {query}")
                    return ErrorResponse(
                        f"Could not create result for following query", error=error
                    )
                except Exception as e:
                    error = Exception(
                        f"Can not get s3 object, with following error"
                    ).with_traceback(e.__traceback__)
                    return ForbiddenErrorResponse(
                        "Unauthorized to perform this request", error=error
                    )

                result = result_obj["Body"].read().decode("utf-8")
                result_no_quotes = (
                    result.replace('"\n"', "\n")
                    .replace('","', ",")
                    .strip('"')
                    .strip('\n"')
                )

                if return_result:
                    if result_format == "json":
                        req_res["result"] = lib.csv_to_json(
                            result_no_quotes, columns_types
                        )
                    else:
                        req_res["result"] = result_no_quotes

            return Response(req_res)
        else:
            return BadRequestErrorResponse(
                "Bad Request:", error=query_serialized.errors
            )


class QuickSightA(GenericAPIView):
    def get(self, request):
        session = boto3.session.Session(
            aws_access_key_id=settings.prod_aws_access_key_id,
            aws_secret_access_key=settings.prod_aws_secret_access_key,
            region_name="eu-west-1",
        )
        client = session.client("quicksight", region_name=settings.aws_region)
        data = client.get_dashboard_embed_url(
            AwsAccountId=settings.prod_account_number,
            DashboardId="f1f53dbf-5029-45a2-b1e8-cca473745e42",
            IdentityType="IAM",
            SessionLifetimeInMinutes=100,
            ResetDisabled=True,
            UndoRedoDisabled=True,
        )
        return Response(data)


class QuickSightB(GenericAPIView):
    def get(self, request):
        session = boto3.session.Session(
            aws_access_key_id=settings.prod_aws_access_key_id,
            aws_secret_access_key=settings.prod_aws_secret_access_key,
            region_name="eu-west-1",
        )
        client = session.client("quicksight", region_name=settings.aws_region)
        data = client.get_dashboard_embed_url(
            AwsAccountId=settings.prod_account_number,
            DashboardId="8dd4682d-ddca-4379-8e5a-6bfc52ec5185",
            IdentityType="IAM",
            SessionLifetimeInMinutes=100,
            ResetDisabled=True,
            UndoRedoDisabled=True,
        )
        return Response(data)


class ActivityViewSet(ModelViewSet):
    serializer_class = ActivitySerializer
    http_method_names = ["get", "head", "post", "delete"]
    filter_fields = ("user", "dataset", "study", "type")

    def get_queryset(self):
        # all activity for all datasets that the user admins
        return Activity.objects.filter(
            dataset_id__in=[x.id for x in self.request.user.admin_datasets.all()]
        )

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        start_raw = request.GET.get("start")
        end_raw = request.GET.get("end")

        if not all([start_raw, end_raw]):
            return ErrorResponse(
                "Please provide start and end as query string params in some datetime format"
            )
        try:
            start = dateparser.parse(start_raw)
            end = dateparser.parse(end_raw)
        except exceptions.ValidationError as e:
            return ErrorResponse(f"Cannot parse this format", error=e)

        queryset = queryset.filter(ts__range=(start, end)).order_by("-ts")
        serializer = self.serializer_class(data=queryset, allow_null=True, many=True)
        serializer.is_valid()

        return Response(serializer.data)

    def create(self, request, *args, **kwargs):
        if request.user.is_execution:
            # replace execution user with real one
            execution = Execution.objects.get(execution_user=request.user)
            request.user = execution.real_user

        activity_serialized = self.serializer_class(data=request.data, allow_null=True)

        if activity_serialized.is_valid():
            # activity_data = activity_serialized.validated_data
            activity = activity_serialized.save()
            activity.user = request.user
            activity.save()
            return Response(
                self.serializer_class(activity, allow_null=True).data, status=201
            )

        else:
            return BadRequestErrorResponse(
                "Bad Request:", error=activity_serialized.errors
            )


class GetExecutionConfig(APIView):
    # noinspection PyMethodMayBeStatic
    def get(self, request):

        execution = Execution.objects.get(execution_user=request.user)
        study = Study.objects.get(execution=execution)
        study_datasets = StudyDataset.objects.filter(study=study)

        config = {"study": StudySerializer(study).data, "datasets": []}
        for study_dataset in study_datasets:
            dataset_ser = DatasetSerializer(study_dataset.dataset).data
            dataset_ser["permission"] = study_dataset.permission
            dataset_ser["data_sources"] = []
            for data_source in study_dataset.dataset.data_sources.all():
                data_source_ser = DataSourceSerializer(data_source).data
                dataset_ser["data_sources"].append(data_source_ser)

            config["datasets"].append(dataset_ser)

        return Response(config)


class Version(APIView):
    # noinspection PyMethodMayBeStatic
    def get(self, request):

        if "study" not in request.query_params:
            return BadRequestErrorResponse("Please provide study as qsp")

        study_id = request.query_params.get("study")

        try:
            study = request.user.studies.get(id=study_id)
        except Study.DoesNotExist as e:
            return NotFoundErrorResponse(
                f"Study {study_id} does not exists or is not permitted", error=e
            )

        start = request.GET.get("start")
        end = request.GET.get("end")

        try:
            if start:
                start = dateparser.parse(start)
            if end:
                end = dateparser.parse(end)
        except exceptions.ValidationError as e:
            return ErrorResponse(f"Can not get list_objects_version.", error=e)

        if (start and end) and not start <= end:
            return ErrorResponse("start > end")

        dataset_from_study = study.datasets.first()
        org_name = dataset_from_study.organization.name

        try:
            items = lib.list_objects_version(
                bucket=study.bucket,
                org_name=org_name,
                filter="*.ipynb",
                exclude=".*",
                start=start,
                end=end,
            )
        except Exception as e:
            return ForbiddenErrorResponse(f"Can not get list_objects_version.", error=e)
        return Response(items)


class DocumentationViewSet(ModelViewSet):
    http_method_names = ["get", "head", "post", "put", "delete"]
    serializer_class = DocumentationSerializer

    def get_serializer(self, *args, **kwargs):
        if "data" in kwargs:
            data = kwargs["data"]

            # check if many is required
            if isinstance(data, list):
                kwargs["many"] = True
        return super(DocumentationViewSet, self).get_serializer(*args, **kwargs)

    def get_queryset(self):
        return self.get_documentation_obj()

    def get_documentation_obj(self):
        if len(self.request.query_params) > 0:
            dataset_id = self.request.query_params["dataset"]
            return Documentation.objects.filter(dataset_id=dataset_id)
        else:
            documentation_id = self.request.parser_context["kwargs"]["pk"]
            return Documentation.objects.filter(id=documentation_id)
