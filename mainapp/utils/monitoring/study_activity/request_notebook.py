import logging
from mainapp.exceptions import InvalidEventData
from mainapp.utils.monitoring.monitor_events import MonitorEvents
from mainapp.utils.monitoring.event_handler import EventHandler


logger = logging.getLogger(__name__)


class RequestNotebook(EventHandler):
    _EVENT_TYPE = MonitorEvents.EVENT_REQUEST_NOTEBOOK

    def __init__(self, data):
        super().__init__(data)
        self.__user = data.get("user")
        self.__study = data.get("study")
        self._user_organization = self.__user.organization.name
        self._user_name = self.__user.display_name

        self._event_args = {
            "study_id": self.__study.id,
            "study_name": self.__study.name,
            "execution_token": self.__study.execution.token
            if self.__study.execution
            else "",
            "environment_name": self.__study.organization.name,
        }

    def _validate_data(self):
        if not self.__study or not self.__user:
            raise InvalidEventData("Request Notebook event must have a study and user")

    def _log_event(self):
        logger.info(
            f"User {self.__user.display_name} from org {self.__user.organization.name} "
            f"has requested the notebook for Study {self.__study.name}:{self.__study.id} "
            f"as jupyter-{self.__study.execution.token if self.__study.execution else ' '}"
        )
