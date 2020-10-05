import logging
import uuid
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet
from mainapp.exceptions import InvalidEc2Status, LaunchTemplateFailedError
from mainapp.models import User, Study, Tag, Execution, Activity, StudyDataset
from mainapp.serializers import StudySerializer
from mainapp.utils import lib, aws_service
from mainapp.utils.monitoring.monitor_events import MonitorEvents
from mainapp.utils.monitoring import handle_event
from mainapp.utils.response_handler import (
    ErrorResponse,
    ForbiddenErrorResponse,
    BadRequestErrorResponse,
)
from mainapp.utils.study_vm_service import (
    delete_study,
    STATUS_ARGS,
    toggle_study_vm,
    setup_study_workspace,
)

logger = logging.getLogger(__name__)


class StudyViewSet(ModelViewSet):
    http_method_names = ["get", "head", "post", "put", "delete"]
    filter_fields = ("user_created",)
    file_types = {
        ".jpg": ["image/jpeg"],
        ".jpeg": ["image/jpeg"],
        ".tiff": ["image/tiff"],
        ".png": ["image/png"],
        ".bmp": ["image/bmp"],
    }

    serializer_class = StudySerializer

    def get_queryset(self, **kwargs):
        user = (
            self.request.user
            if not self.request.user.is_execution
            else Execution.objects.get(execution_user=self.request.user).real_user
        )

        return user.related_studies.exclude(status__exact=Study.STUDY_DELETED)

    @action(detail=True, methods=["get"])
    def get_study_per_organization(self, request, pk=None):
        study = self.get_object()
        organization_name = study.organization.name
        return Response({"study_organization": organization_name})

    def __create_execution(self, study, user):
        """
        Creates an execution object for the created study
        @param study: The created study
        @type study: L{Study}
        @param user: The user that created the study
        @type user: L{User}
        """
        execution_id = uuid.uuid4()

        execution = Execution.objects.create(id=execution_id)
        execution.real_user = user
        execution_user = User.objects.create_user(email=execution.token + "@lynx.md")
        execution_user.set_password(execution.token)
        execution_user.organization = study.organization
        execution_user.is_execution = True
        execution_user.save()
        logger.info(
            f"Created Execution user with identifier: {execution.token} for Study: {study.name}:{study.id} "
            f"in org {study.organization.name}"
        )
        execution.execution_user = execution_user
        execution.save()
        study.execution = execution
        study.save()

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
                status=Study.VM_CREATING,
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

            org_name = study.organization.name

            for dataset in study.datasets.all():
                Activity.objects.create(
                    dataset=dataset,
                    study=study,
                    user=request.user,
                    type="dataset assignment",
                )

            self.__create_execution(study, request.user)
            try:
                setup_study_workspace(
                    org_name=org_name,
                    execution_token=study.execution.token,
                    study_id=study.id,
                )
            except LaunchTemplateFailedError as ce:
                return ErrorResponse(f"Study workspace failed to create", ce)

            handle_event(
                MonitorEvents.EVENT_STUDY_CREATED,
                {"study": study, "view_request": request},
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

            datasets = study_updated["studydataset_set"]

            monitor_kwargs = {"data": {"study": study, "user": user}}

            activity_kwargs = {"study": study, "user": request.user}

            updated_datasets = set(map(lambda x: x["dataset"], datasets))
            existing_datasets = set(study.datasets.all())
            diff_datasets = updated_datasets ^ existing_datasets
            for dataset in diff_datasets:

                monitor_kwargs["data"]["dataset"] = dataset
                monitor_kwargs["data"]["view_request"] = request
                activity_kwargs["dataset"] = dataset

                if dataset in existing_datasets:
                    monitor_kwargs[
                        "event_type"
                    ] = MonitorEvents.EVENT_STUDY_REMOVE_DATASET
                    activity_kwargs["type"] = "dataset remove assignment"

                else:
                    monitor_kwargs["event_type"] = MonitorEvents.EVENT_STUDY_ADD_DATASET
                    activity_kwargs["type"] = "dataset assignment"

                handle_event(**monitor_kwargs)
                Activity.objects.create(**activity_kwargs)

            if study.cover != request.data["cover"]:
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
                            workdir="/tmp/study/",
                            object_key=file_name,
                            local_path=local_path,
                            file_types=self.file_types,
                        )
                    except Exception as e:
                        return BadRequestErrorResponse(error=e)

        return super(self.__class__, self).update(request=self.request)

    def destroy(self, request, *args, **kwargs):
        study = self.get_object()
        delete_study(study)
        return Response(status=204)

    @action(detail=True, methods=["post"], url_path="instance/(?P<status>[^/.]+)")
    def instance(self, request, status, pk=None):
        study = self.get_object()
        try:
            if status not in STATUS_ARGS:
                raise InvalidEc2Status(status)

            logger.info(
                f"Changing study {study.id} ({study.name}) instance {study.execution.execution_user.email} "
                f"state to {status}"
            )

            status_args = STATUS_ARGS[status]

            toggle_study_vm(
                org_name=study.organization.name, study=study, **status_args
            )
            return Response(StudySerializer(study).data, status=201)
        except InvalidEc2Status as ex:
            return BadRequestErrorResponse(message=str(ex))
        except Exception as ex:
            return ErrorResponse(message=str(ex))
