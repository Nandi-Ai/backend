import logging

from mainapp.utils.deidentification.actions.deid_action import DeidentificationAction
from mainapp.utils.deidentification.common.enums import Actions

logger = logging.getLogger(__name__)


class LowerResolution(DeidentificationAction):
    _ACTION_NAME = Actions.LOWER_RESOLUTION.value

    def __init__(self, data_source, dsrc_method, col, lynx_type, **kwargs):
        super().__init__(data_source, dsrc_method, col, lynx_type)
        self._lynx_type.validate_arguments(self._ACTION_NAME, **kwargs)

        self.__action_arguments = kwargs
        self.__action_arguments.update(
            self._col_to_deid.get("additional_attributes", dict())
        )

    def _deid(self, value):
        return self._lynx_type.deid(self._ACTION_NAME, value, **self.__action_arguments)
