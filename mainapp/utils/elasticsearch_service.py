import logging
import uuid

from mainapp import settings
from elasticsearch import Elasticsearch
import elasticsearch
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)

HOST = getattr(settings, "ELASTICSEARCH", dict()).get("host")
if not HOST:
    logger.warning("Elasticsearch monitoring is disabled")


class MonitorEvents(Enum):
    EVENT_USER_LOGIN = "user_login"

    EVENT_REQUEST_NOTEBOOK = "request_notebook"
    EVENT_NOTEBOOK_READY = "notebook_ready"
    EVENT_NOTEBOOK_LOAD_FAIL = "notebook_load_fail"

    EVENT_DATASET_CREATED = "dataset_created"
    EVENT_DATASET_ADD_USER = "dataset_add_user"
    EVENT_DATASET_REMOVE_USER = "dataset_remove_user"
    EVENT_DATASET_DELETED = "dataset_deleted"
    EVENT_DATASET_ADD_DATASOURCE = "dataset_add_datasource"
    EVENT_DATASET_REMOVE_DATASOURCE = "dataset_remove_datasource"
    EVENT_DATASET_ADD_DOCUMENTATION = "dataset_add_documentation"

    EVENT_STUDY_CREATED = "study_created"
    EVENT_STUDY_DELETED = "study_deleted"
    EVENT_STUDY_ASSIGN_DATASET = "study_assign_dataset"


class ElasticsearchService(object):

    __INDEX_NAME = getattr(settings, "ELASTICSEARCH", dict()).get("index")

    __ELASTICSEARCH_CLIENT = None

    @classmethod
    def __get_client(cls):
        if not cls.__ELASTICSEARCH_CLIENT and HOST:
            cls.__ELASTICSEARCH_CLIENT = Elasticsearch(
                hosts=[{"host": HOST, "port": 443, "use_ssl": True}],
                http_auth=(
                    settings.ELASTICSEARCH["auth_user"],
                    settings.ELASTICSEARCH["auth_pass"],
                ),
            )
        return cls.__ELASTICSEARCH_CLIENT

    @classmethod
    def write_monitoring_event(
        cls,
        event_type,
        user_ip="127.0.0.1",
        event_id="",
        dataset_id="",
        study_id="",
        user_name="",
        datasource_id="",
        execution_token="",
        organization_name="",
        additional_data=None,
    ):
        if not HOST:
            return

        event_monitor_object = {
            "event_id": event_id or str(uuid.uuid1()),
            "timestamp": datetime.now(),
            "dataset": dataset_id,
            "study": study_id,
            "user": user_name,
            "user_ip": user_ip,
            "datasource": datasource_id,
            "event_type": event_type.value,
            "organization": organization_name,
            "execution": execution_token,
            "additional_data": additional_data or {},
        }

        index_exists = cls.__get_client().indices.exists(index=cls.__INDEX_NAME)
        if not index_exists:
            logger.warning(
                f"Index {cls.__INDEX_NAME} does not exist. Can not log event"
            )
            return
        try:
            response = cls.__get_client().index(
                index=cls.__INDEX_NAME,
                doc_type="_doc",
                body=event_monitor_object,
                request_timeout=45,
            )
            if response["_shards"]["successful"] == 1:
                logger.info(
                    f"Successfully logged event to Elasticsearch index {cls.__INDEX_NAME}"
                )
        except Exception as e:
            logger.exception("Error while writing to Elasticsearch index", e)

        return