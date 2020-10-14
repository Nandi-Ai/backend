from mainapp.utils.deidentification import LynxDataTypeNames
from mainapp.utils.deidentification.lynx_data_types.date_data_type import Date


class BirthDate(Date):
    _TYPE_NAME = LynxDataTypeNames.BIRTH_DATE.value
