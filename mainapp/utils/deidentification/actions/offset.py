import logging

from mainapp.utils.deidentification.actions.deid_action import DeidentificationAction
from mainapp.utils.deidentification.common.enums import Actions

logger = logging.getLogger(__name__)


class Offset(DeidentificationAction):
    _ACTION_NAME = Actions.OFFSET.value

    def __init__(self, data_source, dsrc_method, col, lynx_type, interval):
        super().__init__(data_source, dsrc_method, col, lynx_type)
        self.__interval = interval

    def _offset(self, value, interval, action_name=Actions.OFFSET.value):
        return self._lynx_type.deid(
            action_name,
            value,
            interval=interval,
            **self._col_to_deid.get("additional_attributes", dict())
        )

    def _deid(self, value):
        return self._offset(value, self.__interval)
