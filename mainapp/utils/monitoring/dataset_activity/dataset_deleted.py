import logging
from mainapp.exceptions import InvalidEventData
from mainapp.utils.monitoring import MonitorEvents
from mainapp.utils.monitoring.event_handler import EventHandler


logger = logging.getLogger(__name__)


class DatasetDeleted(EventHandler):
    _EVENT_TYPE = MonitorEvents.EVENT_DATASET_DELETED

    def __init__(self, data):
        super().__init__(data)
        self.__dataset = data.get("dataset")

        self._event_args = {
            "dataset_id": self.__dataset.id,
            "dataset_name": self.__dataset.name,
            "environment_name": self.__dataset.organization.name,
        }

    def _validate_data(self):
        if not self.__dataset:
            raise InvalidEventData("Dataset Deleted event must have a dataset")

    def _log_event(self):
        logger.info(
            f"User {self._user_name} has deleted "
            f"Dataset {self.__dataset.name}:{self.__dataset.id} "
            f"in org {self.__dataset.organization.name} "
        )
