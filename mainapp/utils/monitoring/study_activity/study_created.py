import logging
from mainapp.exceptions import InvalidEventData
from mainapp.utils.monitoring.monitor_events import MonitorEvents
from mainapp.utils.monitoring.event_handler import EventHandler


logger = logging.getLogger(__name__)


class StudyCreated(EventHandler):
    _EVENT_TYPE = MonitorEvents.EVENT_STUDY_CREATED

    def __init__(self, data):
        super().__init__(data)
        self.__study = data.get("study")

        self._event_args = {
            "study_id": self.__study.id,
            "study_name": self.__study.name,
            "environment_name": self.__study.organization.name,
        }

    def _validate_data(self):
        if not self.__study:
            raise InvalidEventData("Create Study event must have a study")

    def _log_event(self):
        logger.info(
            f"User {self._user_name} has created "
            f"Study {self.__study.name}:{self.__study.id} "
            f"in org {self.__study.organization.name} "
        )
