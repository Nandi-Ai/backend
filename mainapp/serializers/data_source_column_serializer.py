from botocore.exceptions import ClientError
from http import HTTPStatus
from rest_framework.serializers import empty, JSONField, Serializer, ValidationError

from mainapp.utils.deidentification import (
    DATA_TYPE_CASTING,
    LYNX_DATA_TYPES,
    InvalidValueError,
    MismatchingTypesError,
    NoExamplesError,
    COL_NAME_ROW_INDEX,
    EXAMPLE_QUERY_LENGTH,
    EXAMPLE_VALUES_ROW_INDEX,
)


class DataSourceColumnsSerializer(Serializer):
    columns = JSONField()

    def __init__(self, instance=None, data=empty, **kwargs):
        super().__init__(instance, data, **kwargs)
        self.__changed_columns = list()

    def __get_curr_results_from_glue(self, data_source):
        try:
            first_row_query_response = data_source.dataset.query(
                f'SELECT * FROM "{data_source.glue_table}" limit 1;'
            )
        except ClientError as e:
            raise ValidationError(
                f"Could not access original data source {data_source.id},"
                f"error: {e}",
                code=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        try:
            response_object = data_source.dataset.get_query_execution(
                first_row_query_response["QueryExecutionId"]
            )
            query_result = (
                response_object["Body"]
                .read()
                .decode("utf-8")
                .replace('"', "")
                .split("\n")
            )

            if len(query_result) < EXAMPLE_QUERY_LENGTH:
                raise NoExamplesError("Full data is empty, Can't validate data")
            result = dict()
            col_names, first_values = (
                query_result[COL_NAME_ROW_INDEX].split(","),
                query_result[EXAMPLE_VALUES_ROW_INDEX].split(","),
            )
            for col_index in range(len(col_names)):
                result[col_names[col_index]] = first_values[col_index]
        except (ClientError, NoExamplesError) as e:
            raise ValidationError(
                f"Failed parsing values in glue table for data source {data_source.id},"
                f"error: {e}",
                code=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        return result

    def __validate_data_type_to_lynx_type(
        self, col, request_lynx_type, db_lynx_type, data_type
    ):
        if request_lynx_type and request_lynx_type != db_lynx_type:
            self.__changed_columns.append(col)
            LYNX_DATA_TYPES[request_lynx_type].validate_type(data_type)

    def __validate_glue_data(
        self,
        request_data_type,
        db_data_type,
        request_lynx_type,
        db_lynx_type,
        glue_data,
        column_attributes,
    ):
        if db_data_type != request_data_type:
            DATA_TYPE_CASTING[request_data_type](glue_data, *column_attributes.values())
            if request_lynx_type != db_lynx_type:
                LYNX_DATA_TYPES[request_lynx_type]().validate_value(glue_data)

    def get_changed_columns(self):
        return self.__changed_columns

    def validate(self, data):
        data_source = self.context["data_source"]
        columns = data["columns"]
        glue_results = self.__get_curr_results_from_glue(data_source)

        for col, attributes in columns.items():
            col_in_db = data_source.columns.get(col)
            if not col_in_db:
                raise ValidationError(f"column {col} does not exist!")

            try:
                self.__validate_data_type_to_lynx_type(
                    col,
                    attributes.get("lynx_type"),
                    col_in_db["lynx_type"],
                    attributes["data_type"],
                )
                self.__validate_glue_data(
                    attributes.get("data_type"),
                    col_in_db["data_type"],
                    attributes["lynx_type"],
                    col_in_db["data_type"],
                    glue_results[col],
                    attributes.get("additional_attributes", dict()),
                )
            except (MismatchingTypesError, NotImplementedError) as e:
                raise ValidationError(str(e))
            except ValueError:
                raise ValidationError(
                    f"Column {col} cannot be converted "
                    f"into {attributes['data_type']}"
                )
            except InvalidValueError:
                raise ValidationError(
                    f"Column {col} has values which aren't supported by "
                    f"{attributes['lynx_type']}"
                )
        return data
