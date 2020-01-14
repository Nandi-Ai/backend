from time import sleep
from mainapp import settings
from mainapp.exceptions import InvalidExecutionId, QueryExecutionError, MaxExecutionReactedError, \
    UnsupportedColumnTypeError

import boto3
import re


def count_all_values_query(query, glue_database, bucket_name):
    client = boto3.client('athena', region_name=settings.aws_region)
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': glue_database
        },
        ResultConfiguration={
            'OutputLocation': "s3://" + bucket_name + "/temp_execution_results",
        }
    )
    return get_result_query(client, response)


def get_result_query(client, query_execution_result):
    max_execution = 10
    state = 'RUNNING'

    while max_execution > 0 and state == 'RUNNING':
        max_execution -= 1
        query_execution_id = query_execution_result.get('QueryExecutionId')

        if not query_execution_id:
            raise InvalidExecutionId

        response = client.get_query_execution(QueryExecutionId=query_execution_id)

        state = response['QueryExecution']['Status']['State']
        if state == 'FAILED':
            raise QueryExecutionError
        elif state == 'SUCCEEDED':
            try:
                return client.get_query_results(
                    QueryExecutionId=query_execution_id
                )
            except Exception as e:
                raise QueryExecutionError from e
        sleep(1)

    raise MaxExecutionReactedError


def create_sql_for_column(column):
    column_type = column['Type']
    column_name = column['Name']
    numeric_types = ['bigint', 'double']
    sql_as_column_name = replace_given_column_name(column_name)
    if column_type in numeric_types:
        return f"""
            COUNT("{column_name}") as {sql_as_column_name}_Count,
            AVG("{column_name}") as {sql_as_column_name}_AVG,
            COUNT(DISTINCT "{column_name}") as {sql_as_column_name}_Unique,
            APPROX_PERCENTILE("{column_name}",0.25) as {sql_as_column_name}_25_Percentile,
            APPROX_PERCENTILE("{column_name}",0.50) as {sql_as_column_name}_Median,
            APPROX_PERCENTILE("{column_name}",0.75) as {sql_as_column_name}_75_Percentile
            """

    elif column_type == 'string':
        return f"""COUNT("{column_name}") as {sql_as_column_name}_Count, COUNT(DISTINCT "{column_name}") as {sql_as_column_name}_Unique"""
    else:
        raise UnsupportedColumnTypeError


def sql_builder_by_columns_types(glue_table, columns_types):
    queries = list(map(create_sql_for_column, columns_types))
    return f"SELECT {', '.join(queries)} FROM {glue_table};"


def sql_response_processing(query_response):
    sql_metric_names = ['Count', 'AVG', 'Unique', '25_Percentile', 'Median', '75_Percentile']
    metric_names = query_response['ResultSet']['Rows'][0]['Data']
    metric_values = query_response['ResultSet']['Rows'][1]['Data']

    data = [{'sql_metric_name': name} for name in sql_metric_names]

    for metric_name, metric_value in zip(metric_names, metric_values):
        for metric in data:
            if metric_name['VarCharValue'].endswith(metric['sql_metric_name']):
                varCharValue = metric_name['VarCharValue'].replace(metric['sql_metric_name'], "")
                metric.update({varCharValue: metric_value['VarCharValue']})
    return data

def replace_given_column_name(column_name):
    return re.sub(r"[�()-]", " ", column_name).replace(" ", "_")
