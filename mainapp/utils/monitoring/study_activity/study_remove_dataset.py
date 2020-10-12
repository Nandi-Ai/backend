import logging
from mainapp.exceptions import InvalidEventData
from mainapp.utils.monitoring.monitor_events import MonitorEvents
from mainapp.utils.monitoring.event_handler import EventHandler


logger = logging.getLogger(__name__)


class StudyRemoveDataset(EventHandler):
    _EVENT_TYPE = MonitorEvents.EVENT_STUDY_REMOVE_DATASET

    def __init__(self, data):
        super().__init__(data)
        self.__study = data.get("study")
        self.__dataset = data.get("dataset")

        self._event_args = {
            "study_id": self.__study.id,
            "study_name": self.__study.name,
            "dataset_name": self.__dataset.name,
            "dataset_id": self.__dataset.id,
            "environment_name": self.__study.organization.name,
        }

    def _validate_data(self):
        if not self.__study or not self.__dataset:
            raise InvalidEventData(
                "Study Remove Datasset event must have a study and dataset"
            )

    def _log_event(self):
        logger.info(
            f"User {self._user_name} removed "
            f"Dataset {self.__dataset.name}:{self.__dataset.id} from "
            f"Study {self.__study.name}:{self.__study.id} "
            f"in org {self.__study.organization.name} "
        )
