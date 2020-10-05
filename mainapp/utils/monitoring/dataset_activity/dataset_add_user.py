import logging
from mainapp.exceptions import InvalidEventData
from mainapp.utils.monitoring import MonitorEvents
from mainapp.utils.monitoring.event_handler import EventHandler


logger = logging.getLogger(__name__)


class DatasetAddUser(EventHandler):
    _EVENT_TYPE = MonitorEvents.EVENT_DATASET_ADD_USER

    def __init__(self, data):
        super().__init__(data)
        self.__dataset = data.get("dataset")
        self.__additional_data = data.get("additional_data")

        self._event_args = {
            "dataset_id": self.__dataset.id,
            "dataset_name": self.__dataset.name,
            "environment_name": self.__dataset.organization.name,
            "additional_data": self.__additional_data,
        }

    def _validate_data(self):
        if not self.__dataset or not self.__additional_data:
            raise InvalidEventData(
                "Dataset Add User event must have a dataset and user"
            )

    def _log_event(self):
        logger.info(
            f"Added Users "
            f"in Dataset {self.__dataset.name}:{self.__dataset.id} "
            f"in org {self.__dataset.organization.name} "
            f"additional data : {self.__additional_data}"
        )
