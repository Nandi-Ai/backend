import logging

from mainapp.utils.deidentification.actions.deid_action import DeidentificationAction
from mainapp.utils.deidentification.common.enums import Actions

logger = logging.getLogger(__name__)


class Mask(DeidentificationAction):
    _ACTION_NAME = Actions.MASK.value

    def __init__(self, data_source, dsrc_method, col, lynx_type, masked_value):
        super().__init__(data_source, dsrc_method, col, lynx_type)
        self._masked_value = masked_value

    def _mask(self, masked_value):
        return masked_value

    def _deid(self, value):
        return self._mask(self._masked_value)
