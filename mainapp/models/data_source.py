import logging
import uuid

from botocore.exceptions import ClientError
from django.contrib.postgres.fields import JSONField
from django.db import models
from django.db.models import signals
from django.dispatch import receiver

from mainapp.exceptions.limited_key_invalid_exception import LimitedKeyInvalidException
from mainapp.exceptions.s3 import BucketNotFound
from mainapp.utils.data_source import (
    delete_data_source_glue_tables,
    delete_data_source_files_from_bucket,
)
from mainapp.utils.monitoring import handle_event, MonitorEvents
from mainapp.utils.deidentification import (
    GLUE_LYNX_TYPE_MAPPING,
    GLUE_DATA_TYPE_MAPPING,
    GlueDataTypes,
    LynxDataTypeNames,
    DataTypes,
    COL_NAME_ROW_INDEX,
    EXAMPLE_VALUES_ROW_INDEX,
)

logger = logging.getLogger(__name__)
UNAVAILABLE_EXAMPLE = "*N/A*"


class DataSource(models.Model):
    READY = "ready"
    PENDING = "pending"
    ERROR = "error"

    STRUCTURED = "structured"
    IMAGES = "images"
    ZIP = "zip"
    XML = "xml"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    dir = models.CharField(null=True, blank=True, max_length=255)
    s3_objects = JSONField(null=True, blank=True, default=None)
    dataset = models.ForeignKey(
        "Dataset", on_delete=models.CASCADE, related_name="data_sources"
    )
    type = models.CharField(null=True, blank=True, max_length=32)
    about = models.TextField(null=True, blank=True, max_length=2048)
    state = models.CharField(null=True, blank=True, max_length=32)
    programmatic_name = models.CharField(max_length=255, blank=True, null=True)
    ancestor = models.ForeignKey(
        "self", on_delete=models.SET_NULL, related_name="children", null=True
    )
    cohort = JSONField(null=True, blank=True, default=None)
    columns = JSONField(null=True, blank=True, default=None)
    glue_table = models.CharField(null=True, blank=True, max_length=255)

    class Meta:
        db_table = "data_sources"
        unique_together = (("name", "dataset"),)

    @property
    def bucket(self):
        return self.dataset.bucket

    @property
    def permission_key(self):
        dataset = self.dataset
        if dataset.default_user_permission == "limited_access":
            limited = dataset.permission_attributes.get("key")
            try:
                return int(limited)
            except ValueError:
                raise LimitedKeyInvalidException(self)

    @property
    def needs_deid(self):
        methods = list(self.dataset.methods.iterator())
        if self.type not in [self.STRUCTURED, self.IMAGES] or not methods:
            return False

        return not self.methods.exists()

    def generate_columns(self):
        column_types = self.dataset.get_columns_types(self.glue_table)
        self.columns = {
            col["Name"]: {
                "glue_type": col["Type"],
                "data_type": GLUE_DATA_TYPE_MAPPING.get(
                    col["Type"], DataTypes.STRING.value
                ),
                "lynx_type": GLUE_LYNX_TYPE_MAPPING.get(
                    col["Type"], LynxDataTypeNames.TEXT.value
                ),
                "display_name": col["Name"],
            }
            for col in column_types
        }
        self.save()

    def __query_examples(self, columns, query_template):
        column_example_queries = list()
        for col_name, col in columns.items():
            glue_type = col["glue_type"]
            column_example_queries.append(
                query_template.format(
                    col_name=col_name,
                    glue_table=self.glue_table,
                    addition=f" AND \"{col_name}\" <> ''"
                    if glue_type
                    in [
                        GlueDataTypes.CHAR.value,
                        GlueDataTypes.STRING.value,
                        GlueDataTypes.VARCHAR.value,
                    ]
                    else str(),
                )
            )

        logger.info(
            f"Querying table {self.dataset.glue_database}.{self.glue_table} for examples"
        )
        example_values_query_response = self.dataset.query(
            f"SELECT * FROM {','.join(column_example_queries)};"
        )

        response_object = self.dataset.get_query_execution(
            example_values_query_response["QueryExecutionId"]
        )

        logger.info(
            f"Received example values for {self.dataset.glue_database}.{self.glue_table}"
        )
        query_result = (
            response_object["Body"].read().decode("utf-8").replace('"', "").split("\n")
        )

        col_names, example_values = (
            query_result[COL_NAME_ROW_INDEX].split(","),
            query_result[EXAMPLE_VALUES_ROW_INDEX].split(","),
        )
        examples = dict()

        if len(col_names) == len(example_values):
            for col_index in range(len(col_names)):
                examples[col_names[col_index]] = example_values[col_index]

        return examples

    def example_values(self):
        if self.type != self.STRUCTURED or not self.columns:
            logger.warning(
                f"Data Source {self.id} does not have any columns - Could not fetch example values"
            )
            return dict()

        try:
            examples = self.__query_examples(
                self.columns,
                '(SELECT "{col_name}" FROM "{glue_table}" WHERE "{col_name}" IS NOT NULL{addition} limit 1)',
            )
            if not examples:
                logger.warning(
                    f"One of the columns for Data Source {self.id} does not have any values, querying again"
                )
                logger.debug(
                    f"Querying count of actual values for Data Source {self.id}"
                )
                counts = self.__query_examples(
                    self.columns,
                    '(SELECT COUNT(*) as "{col_name}" FROM "{glue_table}" WHERE "{col_name}" IS NOT NULL {addition})',
                )
                columns_with_values = {
                    col_name: col
                    for col_name, col in self.columns.items()
                    if int(counts[col_name])
                }
                examples = self.__query_examples(
                    columns_with_values,
                    '(SELECT "{col_name}" FROM "{glue_table}" WHERE "{col_name}" IS NOT NULL{addition} limit 1)',
                )
                examples.update(
                    {
                        col_name: UNAVAILABLE_EXAMPLE
                        for col_name in self.columns
                        if not columns_with_values.get(col_name)
                    }
                )

        except (ClientError, BucketNotFound) as e:
            logger.error(
                f"Error {e} occurred while trying to fetch example values for Data Source "
                f"{self.name}:{self.id}"
            )
            examples = {col_name: UNAVAILABLE_EXAMPLE for col_name in self.columns}

        return examples

    def __set_state(self, state):
        if self.state == state:
            logger.warning(
                f"Human! Somewhere in your code you're trying to set the data-source {self.id} state "
                f"to {state} when it's already in {state} state."
            )
        else:
            logger.info(f"DataSource {self.id} state was changed to {state}")
            self.state = state
            self.save()

    def set_as_pending(self):
        self.__set_state(DataSource.PENDING)

    def set_as_ready(self):
        self.__set_state(DataSource.READY)

    def set_as_error(self):
        self.__set_state(DataSource.ERROR)

    def is_ready(self):
        return self.state == DataSource.READY

    def is_pending(self):
        return self.state == DataSource.PENDING

    def __get_limited_glue_table_name(self, limited):
        return f"{self.dir}_limited_{limited}"

    def __get_deid_glue_table_name(self, deid):
        return f"{self.dir}_deid_{deid}"

    def get_glue_table(self, permission, key):
        if permission == "limited_access":
            return self.__get_limited_glue_table_name(key)
        elif permission == "deid_access":
            return self.__get_deid_glue_table_name(key)
        else:
            return self.glue_table


@receiver(signals.pre_delete, sender=DataSource)
def delete_data_source(sender, instance, **kwargs):
    data_source = instance
    org_name = instance.dataset.organization.name
    logger.info(
        f"Deleting data source {data_source.name}:{data_source.id}"
        f"for following dataset {data_source.dataset.name}:{data_source.dataset.id}"
        f"in org {data_source.dataset.organization.name}"
    )
    delete_data_source_glue_tables(data_source=data_source, org_name=org_name)
    delete_data_source_files_from_bucket(data_source=data_source, org_name=org_name)

    handle_event(
        MonitorEvents.EVENT_DATASET_REMOVE_DATASOURCE, {"datasource": data_source}
    )
