from abc import ABC, abstractmethod

from mainapp.utils.deidentification.common.exceptions import (
    MismatchingActionError,
    MismatchingTypesError,
    UnsupportedActionArgumentError,
)


class LynxDataType(ABC):
    _SUPPORTED_TYPES = list()
    _SUPPORTED_ACTIONS = dict()
    _TYPE_NAME = None

    @classmethod
    def __validate_basics(cls):
        if not cls._SUPPORTED_TYPES:
            raise NotImplementedError(
                "Lynx Data Types must support specific data types!"
            )

        if not cls._TYPE_NAME:
            raise NotImplementedError(
                "Lynx Data Types must have a name attached to them!"
            )

        if not cls._SUPPORTED_ACTIONS:
            raise NotImplementedError(
                "Lynx Data Types must support only specific actions!"
            )

        if not getattr(cls, "_get_fallback_value"):
            raise NotImplementedError(
                "Lynx Data Types must implement a `_get_fallback_value` method"
            )

    @classmethod
    def validate_type(cls, data_type):
        cls.__validate_basics()

        if data_type not in cls._SUPPORTED_TYPES:
            raise MismatchingTypesError(cls._TYPE_NAME, data_type)

    @classmethod
    def validate_action(cls, action, action_arguments):
        cls.__validate_basics()

        if action not in cls._SUPPORTED_ACTIONS:
            raise MismatchingActionError(cls._TYPE_NAME, action)

        for action_arg in action_arguments:
            if not cls._SUPPORTED_ACTIONS[action]:
                raise UnsupportedActionArgumentError(cls._TYPE_NAME, action, action_arg)
            if action_arg not in cls._SUPPORTED_ACTIONS[action]:
                raise UnsupportedActionArgumentError(cls._TYPE_NAME, action, action_arg)

    @abstractmethod
    def _validate(self, value):
        raise NotImplementedError("Lynx Data Types must implement a _validate method")

    @staticmethod
    def _number_offset(number_in_string, interval):
        int_val = int(number_in_string)
        float_val = float(number_in_string)
        return str(max(int_val, float_val) + interval)

    @classmethod
    def validate_arguments(cls, action, **arguments):
        try:
            getattr(cls, f"_validate_{action}")(**arguments)
        except AttributeError:
            raise NotImplementedError(
                f"Lynx Data Type {cls._TYPE_NAME} does not validate arguments for {action}"
            )

    @classmethod
    def deid(cls, action, value, **arguments):
        try:
            return getattr(cls, f"_{action}")(value, **arguments)
        except AttributeError:
            raise NotImplementedError(
                f"Lynx Data Type {cls._TYPE_NAME} does not implement it's own {action} action"
            )

    @classmethod
    def group_over_age(cls, value, **kwargs):
        return value

    def get_fallback_value(self):
        return getattr(self, "_get_fallback_value")()

    def validate_value(self, value):
        self.__validate_basics()
        if value:
            self._validate(value)
