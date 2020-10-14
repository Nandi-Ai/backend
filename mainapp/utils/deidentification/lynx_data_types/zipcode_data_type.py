from mainapp.utils.deidentification.lynx_data_types.lynx_data_type import LynxDataType
from mainapp.utils.deidentification import LynxDataTypeNames, DataTypes, Actions


class ZipCode(LynxDataType):
    _SUPPORTED_TYPES = [DataTypes.STRING.value]
    _SUPPORTED_ACTIONS = {
        Actions.OMIT.value: None,
        Actions.MASK.value: ["masked_value"],
        Actions.LOWER_RESOLUTION.value: None,
    }
    _TYPE_NAME = LynxDataTypeNames.ZIP_CODE.value
    __RISKY_AREAS = [
        "036",
        "059",
        "102",
        "202",
        "203",
        "204",
        "205",
        "369",
        "556",
        "692",
        "753",
        "772",
        "821",
        "823",
        "878",
        "879",
        "884",
        "893",
    ]

    def _validate(self, value):
        return

    @classmethod
    def _validate_lower_resolution(cls):
        return

    @classmethod
    def _lower_resolution(cls, value):
        if value[:3] in cls.__RISKY_AREAS or value.startswith("0"):
            return "000"
        return value[:3]
