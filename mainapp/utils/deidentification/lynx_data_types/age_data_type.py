import logging

from mainapp.utils.deidentification import (
    LynxDataTypeNames,
    DataTypes,
    Actions,
    GROUP_OVER_AGE_VALUE,
)
from mainapp.utils.deidentification.lynx_data_types.lynx_data_type import LynxDataType

logger = logging.getLogger(__name__)


class Age(LynxDataType):
    _SUPPORTED_TYPES = [DataTypes.FLOAT.value, DataTypes.INT.value]
    _SUPPORTED_ACTIONS = {
        Actions.OMIT.value: None,
        Actions.OFFSET.value: ["interval"],
        Actions.RANDOM_OFFSET.value: ["std"],
        Actions.MASK.value: ["masked_value"],
    }
    _TYPE_NAME = LynxDataTypeNames.AGE.value

    def _validate(self, value):
        return

    @classmethod
    def _offset(cls, value, interval):
        return cls._number_offset(value, interval)

    @classmethod
    def group_over_age(cls, value, **kwargs):
        real_age_val = float(value)
        return str(min(real_age_val, GROUP_OVER_AGE_VALUE))
