from IPy import IP

from mainapp.utils.deidentification.lynx_data_types.lynx_data_type import LynxDataType
from mainapp.utils.deidentification import (
    LynxDataTypeNames,
    DataTypes,
    Actions,
    InvalidValueError,
)


class IPAddress(LynxDataType):
    _SUPPORTED_TYPES = [DataTypes.STRING.value]
    _SUPPORTED_ACTIONS = {
        Actions.OMIT.value: None,
        Actions.MASK.value: ["masked_value"],
    }
    _TYPE_NAME = LynxDataTypeNames.IP_ADDRESS.value

    def _validate(self, value):
        try:
            IP(value)
        except ValueError:
            raise InvalidValueError(self._TYPE_NAME, value)
