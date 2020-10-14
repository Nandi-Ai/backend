import datetime

from dateutil import parser
from dateutil.relativedelta import relativedelta

from mainapp.utils.deidentification import (
    LynxDataTypeNames,
    DataTypes,
    Actions,
    GROUP_OVER_AGE_VALUE,
)
from mainapp.utils.deidentification.common.exceptions import (
    InvalidDeidentificationArguments,
)
from mainapp.utils.deidentification.lynx_data_types.lynx_data_type import LynxDataType


class Date(LynxDataType):
    _SUPPORTED_TYPES = [DataTypes.DATE.value]
    _SUPPORTED_ACTIONS = {
        Actions.OMIT.value: None,
        Actions.OFFSET.value: ["interval"],
        Actions.RANDOM_OFFSET.value: ["std"],
        Actions.MASK.value: ["masked_value"],
        Actions.LOWER_RESOLUTION.value: ["keep_year", "keep_month", "keep_day"],
    }
    _TYPE_NAME = LynxDataTypeNames.DATE.value

    def _get_fallback_value(self):
        return self.__convert_dt_to_string(datetime.datetime.now())

    def _validate(self, value):
        return

    @staticmethod
    def __convert_string_to_dt(value, dt_format):
        return datetime.datetime.strptime(value, dt_format)

    @staticmethod
    def __convert_dt_to_string(dt_obj):
        return dt_obj.isoformat().split("T")[0]

    @classmethod
    def group_over_age(cls, value, **attributes):
        dt_obj = parser.parse(value)

        curr_date = datetime.datetime.now()
        date_diff = relativedelta(dt_obj, curr_date)
        if date_diff.years > GROUP_OVER_AGE_VALUE or (
            date_diff.years == GROUP_OVER_AGE_VALUE
            and any([date_diff.days > 0, date_diff.months > 0])
        ):
            new_year = curr_date.year - (date_diff.years - GROUP_OVER_AGE_VALUE)
            dt_obj = cls.__convert_string_to_dt(f"{new_year}-1-1", "%Y-%m-%d")

        return cls.__convert_dt_to_string(dt_obj)

    @classmethod
    def _offset(cls, value, interval):
        dt_obj = parser.parse(value)
        offset_dt = dt_obj + datetime.timedelta(days=interval)
        return cls.__convert_dt_to_string(offset_dt)

    @classmethod
    def _validate_lower_resolution(
        cls, keep_year=False, keep_month=False, keep_day=False
    ):
        if not keep_year:
            raise InvalidDeidentificationArguments(
                "To lower resolution for a date, the year must be preserved"
            )

        if keep_year and keep_day and not keep_month:
            raise InvalidDeidentificationArguments(
                "Unable to lower resolution for day and year without a month"
            )

    @classmethod
    def _lower_resolution(
        cls, value, keep_year=False, keep_month=False, keep_day=False
    ):
        dt_obj = parser.parse(value)
        low_res_dt = dt_obj
        if all([keep_year, keep_month, keep_day]):
            low_res_dt = f"{dt_obj.year}-{dt_obj.month}-{dt_obj.day}"
        elif keep_year and keep_month:
            low_res_dt = f"{dt_obj.year}-{dt_obj.month}"
        elif keep_year:
            low_res_dt = f"{dt_obj.year}"

        return low_res_dt
