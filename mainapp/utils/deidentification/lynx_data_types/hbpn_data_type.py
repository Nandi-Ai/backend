from mainapp.utils.deidentification.lynx_data_types.lynx_data_type import LynxDataType
from mainapp.utils.deidentification import LynxDataTypeNames, DataTypes, Actions


class HealthPlanBeneficiaryNumber(LynxDataType):
    _SUPPORTED_TYPES = [DataTypes.STRING.value]
    _SUPPORTED_ACTIONS = {
        Actions.OMIT.value: None,
        Actions.MASK.value: ["masked_value"],
        Actions.SALTED_HASH.value: None,
    }
    _TYPE_NAME = LynxDataTypeNames.HPBN.value

    def _get_fallback_value(self):
        return str()

    def _validate(self, value):
        return
