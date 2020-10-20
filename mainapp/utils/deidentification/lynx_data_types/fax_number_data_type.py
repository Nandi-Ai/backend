from mainapp.utils.deidentification.lynx_data_types.lynx_data_type import LynxDataType
from mainapp.utils.deidentification import LynxDataTypeNames, DataTypes, Actions


class FaxNumber(LynxDataType):
    _SUPPORTED_TYPES = [DataTypes.STRING.value]
    _SUPPORTED_ACTIONS = {
        Actions.OMIT.value: None,
        Actions.MASK.value: ["masked_value"],
    }
    _TYPE_NAME = LynxDataTypeNames.FAX_NUMBER.value

    def _get_fallback_value(self):
        return str()

    def _validate(self, value):
        return
