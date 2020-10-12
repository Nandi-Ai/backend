import logging
from mainapp.exceptions import InvalidEventData
from mainapp.utils.monitoring import MonitorEvents
from mainapp.utils.monitoring.event_handler import EventHandler


logger = logging.getLogger(__name__)


class DatasetAddDatasource(EventHandler):
    _EVENT_TYPE = MonitorEvents.EVENT_DATASET_ADD_DATASOURCE

    def __init__(self, data):
        super().__init__(data)
        self.__datasource = data.get("datasource")
        self.__dataset = self.__datasource.dataset

        self._event_args = {
            "dataset_id": self.__datasource.dataset.id,
            "dataset_name": self.__dataset.name,
            "datasource_name": self.__datasource.name,
            "datasource_id": self.__datasource.id,
            "environment_name": self.__dataset.organization.name,
        }

    def _validate_data(self):
        if not self.__datasource:
            raise InvalidEventData(
                "Dataset Add Datasource event must have a datasource"
            )

    def _log_event(self):
        logger.info(
            f"User {self._user_name} has added "
            f"Datasource {self.__datasource.name}:{self.__datasource.id}"
            f"to Dataset {self.__dataset.name}:{self.__dataset.id} "
            f"in org {self.__dataset.organization.name} "
        )
