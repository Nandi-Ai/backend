import logging
from mainapp.exceptions import InvalidEventData
from mainapp.utils.monitoring.monitor_events import MonitorEvents
from mainapp.utils.monitoring.event_handler import EventHandler


logger = logging.getLogger(__name__)


class UserSignedEula(EventHandler):
    _EVENT_TYPE = MonitorEvents.EVENT_USER_SIGNED_EULA

    def __init__(self, data):
        super().__init__(data)
        self.__user = data.get("user")
        self._user_name = self.__user.display_name
        self._user_organization = self.__user.organization.name

    def _validate_data(self):
        if not self.__user:
            raise InvalidEventData("User signed eula event must have a user")

    def _log_event(self):
        logger.info(
            f"User {self.__user.display_name} from org {self.__user.organization.name} has signed eula"
        )
