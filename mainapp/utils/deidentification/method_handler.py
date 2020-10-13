import logging
import s3fs

from . import ACTIONS, LYNX_DATA_TYPES
from mainapp.settings import ORG_VALUES
from mainapp.utils.lib import create_deid_glue_table
from mainapp.utils.aws_service import create_s3_client
from mainapp.utils.deidentification.common.enums import Actions

logger = logging.getLogger(__name__)


class MethodHandler(object):
    def __init__(self, data_source, dsrc_method, data_source_index):
        self.__dsrc_index = data_source_index
        self.__data_source = data_source
        self.__dsrc_method = dsrc_method
        self.__method_id = self.__dsrc_method.method.id
        self.__deid_data_dir = f"{self.__data_source.dir}/lynx-storage/deid_access_{self.__method_id}_{self.__dsrc_index}"
        self.__aws_access_key = ORG_VALUES[data_source.dataset.organization.name][
            "AWS_ACCESS_KEY_ID"
        ]
        self.__aws_secret = ORG_VALUES[data_source.dataset.organization.name][
            "AWS_SECRET_ACCESS_KEY"
        ]
        self.__actions = {
            col: ACTIONS[col_attributes["action"]](
                data_source=data_source,
                dsrc_method=dsrc_method,
                col=col_attributes,
                lynx_type=LYNX_DATA_TYPES[col_attributes["lynx_type"]],
                **col_attributes["arguments"],
            )
            for col, col_attributes in dsrc_method.attributes.items()
        }

    def __fetch_data_object_from_glue(self):
        query_response = self.__data_source.dataset.query(
            f'SELECT * FROM "{self.__data_source.glue_table}";'
        )
        return self.__data_source.dataset.get_query_execution(
            query_response["QueryExecutionId"]
        )["Body"]

    @staticmethod
    def __decode_stream_row(row):
        return row.decode("utf-8").rstrip("\n").replace('"', "").split(",")

    @staticmethod
    def __encode_deid_row(row):
        return f"{','.join(row)}\n".encode("utf-8")

    def __create_s3_deid_bucket(self):
        s3_client = create_s3_client(
            org_name=self.__data_source.dataset.organization.name
        )
        s3_client.put_object(
            Bucket=self.__data_source.bucket,
            Key=f"{self.__deid_data_dir}/",
            ACL="private",
        )
        s3_client.put_object(
            Bucket=self.__data_source.bucket,
            Key=f"{self.__deid_data_dir.rstrip(f'_{self.__dsrc_index}')}/",
            ACL="private",
        )

    def __deidentify_col_name_row(self, row, columns):
        deid_row = list()
        for col in columns:
            col_index = columns[col]
            original_value = row[col_index]
            action = self.__actions.get(col)
            if not action:
                deid_row.append(original_value)
                continue

            deid_value = action.deid_column_names(row, col_index)
            if deid_value or not original_value:
                deid_row.append(deid_value)

        return deid_row

    def __deidentify_row(self, data_row, columns):
        data_row = self.__decode_stream_row(data_row)
        deid_row = list()
        final_actions = dict()
        replacement_cache = dict()

        for col in columns:
            original_col_index = columns[col]
            original_value = data_row[original_col_index]
            action = self.__actions.get(col)
            if not action:
                deid_row.append(original_value)
                continue

            if action.name == Actions.FREE_TEXT_REPLACEMENT.value:
                deid_row.append(original_value)
                final_actions[col] = (action, len(deid_row) - 1)
                continue

            deid_value = action.deid(original_value)
            if deid_value or not original_value:
                deid_row.append(deid_value)

            replacement_cache[original_value] = deid_value or str()

        for col, action_data in final_actions.items():
            original_value = data_row[columns[col]]
            action, action_col_index = action_data[0], action_data[1]
            action.update_mapping(replacement_cache)
            deid_row[action_col_index] = action.deid(original_value)

        return deid_row

    def __communicate_with_bucket(self, data_stream, column_name_row):
        columns = {name: idx for idx, name in enumerate(column_name_row)}
        self.__create_s3_deid_bucket()
        logger.info(
            f"Deidentifying column names for Data Source {self.__data_source.name}:{self.__data_source.id}"
        )

        column_name_row = self.__deidentify_col_name_row(column_name_row, columns)
        logger.info(
            f"Deidentifying data for Data Source {self.__data_source.name}:{self.__data_source.id}"
        )

        s3 = s3fs.S3FileSystem(
            anon=False, key=self.__aws_access_key, secret=self.__aws_secret
        )

        deid_data_file = f"s3://{self.__data_source.bucket}/{self.__deid_data_dir}/{self.__data_source.name}"
        with s3.open(deid_data_file, "wb") as deid_result:
            deid_result.write(self.__encode_deid_row(column_name_row))
            for data_row in data_stream._raw_stream:
                deid_row = self.__deidentify_row(data_row, columns)
                deid_result.write(self.__encode_deid_row(deid_row))

        logger.info(f"Uploaded Deidentified file to {deid_data_file}")

    def apply(self):
        if not self.__actions:
            logger.warning(
                f"Method {self.__dsrc_method.method.name}:{self.__dsrc_method.method.id} has no actions for"
                f"Data Source {self.__data_source.name}:{self.__data_source.id}"
            )
            self.__dsrc_method.state = "ready"
            self.__dsrc_method.save()
            return

        try:
            logger.info(
                f"Fetching entire data from glue table "
                f"{self.__data_source.dataset.glue_database}.{self.__data_source.glue_table}"
            )
            data_stream = self.__fetch_data_object_from_glue()

            logger.info(
                f"Reading column name row for Data Source {self.__data_source.name}:{self.__data_source.id}"
            )
            column_name_row = self.__decode_stream_row(
                data_stream._raw_stream.readline()
            )

            self.__communicate_with_bucket(data_stream, column_name_row)

            logger.info(
                f"Creating Deidentified glue table for Data Source {self.__data_source.name}:{self.__data_source.id} "
                f"for Method {self.__dsrc_method.method.name}:{self.__dsrc_method.method.name}"
            )

            create_deid_glue_table(
                data_source=self.__data_source,
                deid=self.__dsrc_method.method.id,
                dsrc_index=self.__dsrc_index,
            )

            self.__dsrc_method.state = "ready"

        except Exception as e:
            logger.exception(
                f"Error occurred when applying deid method "
                f"{self.__dsrc_method.method.name}: {self.__dsrc_method.method.id} "
                f"on data source {self.__data_source.name}: {self.__data_source.id} - \nerror: {e}"
            )
            self.__dsrc_method.state = "error"
        finally:
            self.__dsrc_method.save()
