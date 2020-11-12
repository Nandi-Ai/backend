import logging

from rest_framework.generics import GenericAPIView
from rest_framework.response import Response

from mainapp.models import Study, Dataset, Activity
from mainapp.serializers import SimpleQuerySerializer
from mainapp.utils import lib, aws_service
from mainapp.utils.response_handler import ErrorResponse, ForbiddenErrorResponse
from mainapp.utils.aws_utils.s3_storage import TEMP_EXECUTION_DIR

logger = logging.getLogger(__name__)


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

            access = dataset.calc_access_to_database(execution.real_user)

            permission = access["permission"]

            if permission == "aggregated access":
                if not lib.is_aggregated(query_string):
                    return ErrorResponse(
                        "This is not an aggregated query. Only aggregated queries are allowed"
                    )

            if permission == "no access":
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
                        "OutputLocation": f"s3://{dataset.full_path}/{TEMP_EXECUTION_DIR}"
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

            logger.info(
                f"Running Query for Activity: {query_string} on dataset: {dataset.name}:{dataset.id} "
                f"in org {dataset.organization.name} by user: {request.user.display_name}"
            )

            return Response({"query_execution_id": response["QueryExecutionId"]})
        else:
            return query_serialized.errors
