import json
import logging

from django.db.utils import IntegrityError
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response

from mainapp.models import Dataset, DataSource
from mainapp.serializers import CohortSerializer
from mainapp.utils import devexpress_filtering
from mainapp.utils import lib, aws_service
from mainapp.utils.elasticsearch_service import MonitorEvents, ElasticsearchService
from mainapp.utils.response_handler import (
    ErrorResponse,
    ForbiddenErrorResponse,
    BadRequestErrorResponse,
)

logger = logging.getLogger(__name__)


class CreateCohort(GenericAPIView):
    serializer_class = CohortSerializer

    def __monitor_cohort(self, event_type, user_ip, datasource, user):
        ElasticsearchService.write_monitoring_event(
            event_type=event_type,
            user_ip=user_ip,
            dataset_id=datasource.dataset.id,
            user_name=user.display_name,
            datasource_id=datasource,
            organization_name=datasource.dataset.organization.name,
        )

        logger.info(
            f"Cohort Event: {event_type.value} "
            f"on dataset {datasource.dataset.name}:{datasource.dataset.id} "
            f"and datasource {datasource.name}:{datasource.id}"
            f"by user {user.display_name} "
            f"in org {datasource.dataset.organization}"
        )

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
                self.__monitor_cohort(
                    event_type=MonitorEvents.EVENT_DATASET_ADD_DATASOURCE,
                    user_ip=lib.get_client_ip(request),
                    datasource=new_data_source,
                    user=request.user,
                )

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
