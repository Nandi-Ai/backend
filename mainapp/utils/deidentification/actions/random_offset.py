import logging

from random import gauss

from mainapp.utils.deidentification.actions.offset import Offset
from mainapp.utils.deidentification.common.enums import Actions

logger = logging.getLogger(__name__)


class RandomOffset(Offset):
    _ACTION_NAME = Actions.RANDOM_OFFSET.value

    def __init__(self, data_source, dsrc_method, col, lynx_type, std):
        super(Offset, self).__init__(data_source, dsrc_method, col, lynx_type)
        self.__std = std

    def _deid(self, value):
        interval = max(min(3 * self.__std, gauss(0, self.__std)), -3 * self.__std)
        return self._offset(value, interval)
