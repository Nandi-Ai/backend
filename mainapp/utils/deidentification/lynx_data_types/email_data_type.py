from email_validator import EmailSyntaxError, validate_email

from mainapp.utils.deidentification import (
    LynxDataTypeNames,
    DataTypes,
    Actions,
    InvalidValueError,
)
from mainapp.utils.deidentification.lynx_data_types.lynx_data_type import LynxDataType


class Email(LynxDataType):
    _SUPPORTED_TYPES = [DataTypes.STRING.value]
    _SUPPORTED_ACTIONS = {
        Actions.OMIT.value: None,
        Actions.MASK.value: ["masked_value"],
    }
    _TYPE_NAME = LynxDataTypeNames.EMAIL.value

    def _validate(self, value):
        try:
            validate_email(value, check_deliverability=False)
        except EmailSyntaxError:
            raise InvalidValueError(self._TYPE_NAME, value)
