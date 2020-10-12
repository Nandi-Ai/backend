class NoExamplesError(Exception):
    pass


class DeidentificationError(Exception):
    def __init__(self, lynx_data_type):
        self._lynx_data_type = lynx_data_type


class InvalidValueError(DeidentificationError):
    def __init__(self, lynx_data_type, value):
        super().__init__(lynx_data_type)
        self.__value = value

    def __str__(self):
        return f"{self.__value} is not valid for data type {self._lynx_data_type}"


class MismatchingTypesError(DeidentificationError):
    def __init__(self, lynx_data_type, data_type):
        super().__init__(lynx_data_type)
        self.__data_type = data_type

    def __str__(self):
        return f"Lynx Data Type {self._lynx_data_type} does not support {self.__data_type} conversion"


class MismatchingActionError(DeidentificationError):
    def __init__(self, lynx_data_type, action):
        super().__init__(lynx_data_type)
        self.__action = action

    def __str__(self):
        return f"Lynx Data Type {self._lynx_data_type} does not support {self.__action}"


class UnsupportedActionArgumentError(DeidentificationError):
    def __init__(self, lynx_data_type, action, argument):
        super().__init__(lynx_data_type)
        self.__action = action
        self.__argument = argument

    def __str__(self):
        return f"Action {self.__action} does not accept argument {self.__argument} in {self._lynx_data_type}"


class InvalidDeidentificationArguments(DeidentificationError):
    pass
