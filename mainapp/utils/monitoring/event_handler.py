import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from elasticsearch import Elasticsearch
from mainapp import settings
from mainapp.utils import lib
from mainapp.exceptions import InvalidEventData
import logging


logger = logging.getLogger(__name__)


HOST = getattr(settings, "ELASTICSEARCH", dict()).get("host")
if not HOST:
    logger.warning("Elasticsearch monitoring is disabled")


class EventHandler(ABC):
    class __ElasticsearchService(object):
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
            dataset_name="",
            study_id="",
            study_name="",
            user_name="",
            datasource_id="",
            datasource_name="",
            execution_token="",
            user_organization="",
            environment_name="",
            additional_data=None,
        ):
            if not HOST:
                return

            event_monitor_object = {
                "event_id": event_id or str(uuid.uuid1()),
                "timestamp": datetime.now(),
                "user_name": user_name,
                "user_organization": user_organization,
                "environment_name": environment_name,
                "event_type": event_type.value,
                "study_name": study_name,
                "dataset_name": dataset_name,
                "datasource_name": datasource_name,
                "additional_data": additional_data or {},
                "user_ip": user_ip,
                "study_id": study_id,
                "execution": execution_token,
                "dataset_id": dataset_id,
                "datasource_id": datasource_id,
            }
            try:
                index_exists = cls.__get_client().indices.exists(index=cls.__INDEX_NAME)
                if not index_exists:
                    logger.warning(
                        f"Index {cls.__INDEX_NAME} does not exist. Can not log event"
                    )
                    return
            except Exception as e:
                logger.exception("Error while connecting to Elasticsearch index", e)
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
                        f"Successfully logged {event_type} event to Elasticsearch index {cls.__INDEX_NAME}"
                    )
            except Exception as e:
                logger.exception("Error while writing to Elasticsearch index", e)

            return

    __ELASTIC_SERVICE = __ElasticsearchService()
    _EVENT_TYPE = None

    def __init__(self, data):
        self.__user_ip = "127.0.0.1"
        self._user_organization = ""
        self._user_name = ""
        self._event_args = dict()

        if "view_request" in data.keys():
            self.__user_ip = lib.get_client_ip(data.get("view_request"))
            self._user_organization = data.get("view_request").user.organization.name
            self._user_name = data.get("view_request").user.display_name

    @abstractmethod
    def _validate_data(self):
        raise NotImplementedError("A _validate_data method must be implemented")

    def __write_event(self):
        self.__ELASTIC_SERVICE.write_monitoring_event(
            event_type=self._EVENT_TYPE,
            user_ip=self.__user_ip,
            user_name=self._user_name,
            user_organization=self._user_organization,
            **self._event_args,
        )

    @abstractmethod
    def _log_event(self):
        raise NotImplementedError("A _log_event method must be implemented")

    def invoke(self):
        try:
            if not self._EVENT_TYPE:
                raise InvalidEventData("All events must have a type")
            self._validate_data()
            self.__write_event()
        except InvalidEventData as ied:
            logger.exception(f"The data given for {self.__class__} failed due to {ied}")
            return
        self._log_event()
