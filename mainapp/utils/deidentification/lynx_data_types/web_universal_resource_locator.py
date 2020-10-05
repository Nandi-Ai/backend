import logging

from mainapp.utils.deidentification.lynx_data_types.lynx_data_type import LynxDataType
from mainapp.utils.deidentification import LynxDataTypeNames, DataTypes, Actions

logger = logging.getLogger(__name__)


class WebUniversalResourceLocator(LynxDataType):
    _SUPPORTED_TYPES = [DataTypes.STRING.value]
    _SUPPORTED_ACTIONS = {Actions.OMIT.value: None}
    _TYPE_NAME = LynxDataTypeNames.WURL.value

    def _validate(self, value):
        return
