import logging

from mainapp.utils.deidentification.actions.deid_action import DeidentificationAction
from mainapp.utils.deidentification.common.enums import Actions

logger = logging.getLogger(__name__)


class Omission(DeidentificationAction):
    _ACTION_NAME = Actions.OMIT.value

    def deid_column_names(self, column_names_row, col_index):
        return None

    def _deid(self, value):
        return None
