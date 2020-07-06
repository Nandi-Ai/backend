import json
import logging
import time

import botocore.exceptions
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

from mainapp import resources, settings
from mainapp.models import User, Study, Tag, Execution, Activity, StudyDataset
from mainapp.serializers import StudySerializer
from mainapp.utils import lib, aws_service
from mainapp.utils.elasticsearch_service import MonitorEvents, ElasticsearchService
from mainapp.utils.response_handler import (
    ErrorResponse,
    ForbiddenErrorResponse,
    UnimplementedErrorResponse,
)

logger = logging.getLogger(__name__)


class StudyViewSet(ModelViewSet):
    http_method_names = ["get", "head", "post", "put", "delete"]
    filter_fields = ("user_created",)

    serializer_class = StudySerializer

    def __monitor_study(self, study, user_ip, event_type, user, dataset=None):
        ElasticsearchService.write_monitoring_event(
            user_ip=user_ip,
            study_name=study.name,
            study_id=study.id,
            event_type=event_type,
            user_name=user.display_name,
            environment_name=study.organization.name,
            user_organization=user.organization.name,
            dataset_id=dataset.id if dataset else "",
            dataset_name=dataset.name if dataset else "",
        )

        dataset_log = f"and dataset {dataset.name}:{dataset.id}" if dataset else ""
        logger.info(
            f"Study Event: {event_type.value} "
            f"on study {study.name}:{study.id} "
            f"{dataset_log}"
            f"by user {user.display_name} "
            f" in org {study.organization.name}"
        )

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
        user = (
            self.request.user
            if not self.request.user.is_execution
            else Execution.objects.get(execution_user=self.request.user).real_user
        )

        return user.related_studies.all()

    # @transaction.atomic
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

            # set the study organization same as the first dataset
            first_dataset_organization = req_datasets[0]["dataset"].organization

            study = Study.objects.create(
                name=study_name,
                organization=first_dataset_organization,
                cover=study_serialized.validated_data.get("cover"),
            )
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
            self.__monitor_study(
                event_type=MonitorEvents.EVENT_STUDY_CREATED,
                user_ip=lib.get_client_ip(request),
                user=request.user,
                study=study,
            )

            workspace_bucket_name = study.bucket
            org_name = study.organization.name
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
            user = (
                request.user
                if not request.user.is_execution
                else Execution.objects.get(execution_user=request.user).real_user
            )
            if user not in study.users.all():
                return ForbiddenErrorResponse(
                    f"Only the study creator can edit a study"
                )

            org_name = study.organization.name

            client = aws_service.create_iam_client(org_name=org_name)
            account_number = settings.ORG_VALUES[org_name]["ACCOUNT_NUMBER"]
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
                self.__monitor_study(
                    event_type=MonitorEvents.EVENT_STUDY_ASSIGN_DATASET,
                    user_ip=lib.get_client_ip(request),
                    study=study,
                    user=request.user,
                    dataset=d,
                )
                Activity.objects.create(
                    type="dataset assignment", study=study, dataset=d, user=user
                )

        return super(self.__class__, self).update(request=self.request)
