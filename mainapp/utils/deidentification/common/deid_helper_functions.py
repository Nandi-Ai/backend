import logging

from mainapp.models import DataSource
from mainapp.utils import executor
from mainapp.utils.deidentification.common.exceptions import (
    DeidentificationError,
    MismatchingActionError,
    UnsupportedActionArgumentError,
)
from mainapp.utils.deidentification import LYNX_DATA_TYPES
from mainapp.utils.deidentification.method_handler import MethodHandler
from mainapp.utils.deidentification.images_de_id import ImageDeId

logger = logging.getLogger(__name__)


def validate_action(method_action, column, data_source, dsrc_method, attributes):
    try:
        logger.debug(
            f"Validating Action {method_action} for column {column} in "
            f"Method {dsrc_method.method.name}:{dsrc_method.method.id} over "
            f"Data Source {data_source.name}:{data_source.id}"
        )

        LYNX_DATA_TYPES[data_source.columns[column]["lynx_type"]].validate_action(
            method_action, attributes["arguments"].keys()
        )
    except MismatchingActionError as mae:
        logger.exception(
            f"Unsupported Action {attributes['action']} given to "
            f"lynx data type {data_source.columns[column]['lynx_type']}"
        )
        raise DeidentificationError(str(mae))
    except UnsupportedActionArgumentError as uaae:
        logger.exception(
            f"Action {attributes['action']} does not support the arguments given for"
            f"lynx data type {data_source.columns[column]['lynx_type']}"
        )
        raise DeidentificationError(uaae)
    dsrc_method.set_as_pending()


def handle_method(dsrc_method, data_source, data_source_index):
    if not dsrc_method.included:
        logger.debug(
            f"Method {dsrc_method.method.name}:{dsrc_method.method.id} does not include "
            f"Data Source {data_source.name}:{data_source.id} - Skipping"
        )
        return

    try:
        if data_source.type == DataSource.STRUCTURED:
            logger.debug(
                f"Going over column actions for Method {dsrc_method.method.name}:{dsrc_method.method.id} over "
                f"{data_source.name}:{data_source.id}"
            )

            for col, attributes in dsrc_method.attributes.items():
                validate_action(
                    attributes["action"], col, data_source, dsrc_method, attributes
                )

            try:
                logger.info(
                    f"Handling Method {dsrc_method.method.name}:{dsrc_method.method.id} for "
                    f"Data Source {data_source.name}:{data_source.id}"
                )
                handler = MethodHandler(data_source, dsrc_method, data_source_index)
                executor.submit(handler.apply)
            except Exception as e:
                logger.error(
                    f"Failed to create Method Handler for Method {dsrc_method.method.name}:{dsrc_method.method.id} "
                    f"for Data Source {data_source.name}:{data_source.id}, error - {e}"
                )
                raise
        elif data_source.type == DataSource.IMAGES:
            logger.info(
                f"Handling Image De-id for Method {dsrc_method.method.name}:{dsrc_method.method.id} for "
                f"{data_source.name}:{data_source.id}"
            )

            if dsrc_method.included:
                image_de_id = ImageDeId(
                    org_name=data_source.dataset.organization.name,
                    data_source=data_source,
                    dsrc_method=dsrc_method,
                )
                image_de_id.image_de_identification()
    except Exception as e:
        logger.error(
            f"Failed to create Method Handler for Method {dsrc_method.method.name}:{dsrc_method.method.id} "
            f"for Data Source {data_source.name}:{data_source.id}, error - {e}"
        )
        raise
