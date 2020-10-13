from mainapp.utils.deidentification import LynxDataTypeNames, DataTypes, Actions
from mainapp.utils.deidentification.lynx_data_types.lynx_data_type import LynxDataType


class Number(LynxDataType):
    _SUPPORTED_TYPES = [DataTypes.FLOAT.value, DataTypes.INT.value]
    _SUPPORTED_ACTIONS = {
        Actions.OMIT.value: None,
        Actions.OFFSET.value: ["interval"],
        Actions.RANDOM_OFFSET.value: ["std"],
        Actions.MASK.value: ["masked_value"],
    }
    _TYPE_NAME = LynxDataTypeNames.NUMBER.value

    def _validate(self, value):
        return

    @classmethod
    def _offset(cls, value, interval):
        return cls._number_offset(value, interval)
