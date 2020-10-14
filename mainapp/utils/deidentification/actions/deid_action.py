import logging

from abc import ABC, abstractmethod


logger = logging.getLogger(__name__)


class DeidentificationAction(ABC):
    _ACTION_NAME = None

    def __init__(self, data_source, dsrc_method, col, lynx_type):
        if not self._ACTION_NAME:
            raise NotImplementedError(
                "Every deidentification must have an assigned name!"
            )

        self._col_to_deid = col
        self._data_source = data_source
        self._lynx_type = lynx_type
        self._dsrc_method = dsrc_method
        logger.info(
            f"Created Deidentification Action {self._ACTION_NAME} for Data Source "
            f"{self._data_source.name}:{self._data_source.id}"
        )

    def deid_column_names(self, column_names_row, col_index):
        return column_names_row[col_index]

    @abstractmethod
    def _deid(self, value):
        raise NotImplementedError(
            "Every De-Identification action must implement a deid method!"
        )

    def get_fallback_value(self):
        return self._lynx_type.get_fallback_value()

    def deid(self, value):
        if self._dsrc_method.method.group_age_over:
            value = self._lynx_type.group_over_age(value)

        return self._deid(value)

    @property
    def name(self):
        return self._ACTION_NAME
