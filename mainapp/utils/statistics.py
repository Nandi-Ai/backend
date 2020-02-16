from time import sleep
from mainapp import settings
from mainapp.exceptions import InvalidExecutionId, QueryExecutionError, MaxExecutionReactedError, \
    UnsupportedColumnTypeError

import boto3
import re


def count_all_values_query(query, glue_database, bucket_name):
    client = boto3.client('athena', region_name=settings.AWS['AWS_REGION'])
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


def max_count(response):
    metric_names = response['ResultSet']['Rows'][0]['Data']
    metric_values = response['ResultSet']['Rows'][1]['Data']

    return max([
        int(metric_value['VarCharValue'])
        for metric_name, metric_value in zip(metric_names, metric_values)
        if metric_name['VarCharValue'].rsplit('_', 1)[1] == 'Count'
    ])


def create_sql_for_column(column):
    numeric_types = ['bigint', 'double']
    column_name_as_in_file = column[0]['Name']
    athena_default_column_name = column[0]['Col_Name']
    column_name_from_athena = column[1]['Name']
    column_type_from_athena = column[1]['Type']
    sql_as_column_name = replace_given_column_name(athena_default_column_name)
    if column_type_from_athena in numeric_types and column_name_as_in_file == column_name_from_athena:
        return f"""
            COUNT("{column_name_from_athena}") as {sql_as_column_name}_Count,
            COUNT(DISTINCT "{column_name_from_athena}") as {sql_as_column_name}_Unique,
            AVG("{column_name_from_athena}") as {sql_as_column_name}_Average,
            APPROX_PERCENTILE("{column_name_from_athena}",0.25) as "{sql_as_column_name}_25%",
            APPROX_PERCENTILE("{column_name_from_athena}",0.50) as "{sql_as_column_name}_50%",
            APPROX_PERCENTILE("{column_name_from_athena}",0.75) as "{sql_as_column_name}_75%"
            """

    elif column_type_from_athena == 'string':
        return f"""COUNT("{column_name_from_athena}") as {sql_as_column_name}_Count,
                COUNT(DISTINCT "{column_name_from_athena}") as {sql_as_column_name}_Unique"""
    else:
        raise UnsupportedColumnTypeError


def sql_builder_by_columns_types(glue_table, columns_types, default_athena_col_names, filter_query):
    queries = list(map(create_sql_for_column, zip(default_athena_col_names, columns_types)))
    final_query = f"""SELECT {', '.join(queries)} FROM "{glue_table}" """
    if filter_query:
        final_query += f""" {filter_query}"""
    return f"""{final_query};"""


def sql_response_processing(query_response, default_athena_col_names):
    metric_names = query_response['ResultSet']['Rows'][0]['Data']
    metric_values = query_response['ResultSet']['Rows'][1]['Data']

    temp_result = {}
    for metric_name, metric_value in zip(metric_names, metric_values):
        if metric_value:
            metric_name_list = metric_name['VarCharValue'].rsplit('_', 1)
            metric_col_name, metric = metric_name_list[0], metric_name_list[1]
            for col_name in default_athena_col_names:
                if metric_col_name == col_name['Col_Name']:
                    if metric not in temp_result:
                        temp_result[metric] = {}

                    varCharValue = col_name['Name']
                    temp_result[metric][varCharValue] = metric_value['VarCharValue']

    return [x for x in temp_result.values()]


def replace_given_column_name(column_name):
    return re.sub(r"[�()-]", " ", column_name).replace(" ", "_")


def create_default_column_names(column_names):
    return [{'Name': name['Name'], 'Col_Name': f"col{i}"} for i, name in enumerate(column_names)]
