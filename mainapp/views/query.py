import json
import logging

from rest_framework.generics import GenericAPIView
from rest_framework.response import Response

from mainapp.exceptions import BucketNotFound
from mainapp.models import Dataset, DataSource
from mainapp.serializers import QuerySerializer
from mainapp.utils import devexpress_filtering
from mainapp.utils import lib
from mainapp.utils.response_handler import (
    ErrorResponse,
    ForbiddenErrorResponse,
    NotFoundErrorResponse,
    BadRequestErrorResponse,
)

logger = logging.getLogger(__name__)


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
            logger.info(
                f"Executing Query : {final_query} "
                f"on dataset: {dataset.name}:{dataset.id} "
                f"and datasource: {data_source.name}:{data_source.id} "
                f"in org {dataset.organization.name} "
                f"by user: {request.user.display_name}"
            )

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
                logger.info(
                    f"Final query {final_query} on Dataset {dataset.name}:{dataset.id} in org {dataset.organization.name}"
                )
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
                    logger.info(
                        f"No result for query: {query} on Dataset {dataset.name}:{dataset.id} "
                        f"in org {dataset.organization.name}"
                    )
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
