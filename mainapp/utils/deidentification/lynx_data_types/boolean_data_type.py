from mainapp.utils.deidentification.lynx_data_types.lynx_data_type import LynxDataType
from mainapp.utils.deidentification import LynxDataTypeNames, DataTypes, Actions


class Boolean(LynxDataType):
    _SUPPORTED_TYPES = [DataTypes.BOOLEAN.value]
    _SUPPORTED_ACTIONS = {Actions.OMIT.value: None}
    _TYPE_NAME = LynxDataTypeNames.BOOLEAN.value

    def _get_fallback_value(self):
        return bool()

    def _validate(self, value):
        return
