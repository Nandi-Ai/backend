from mainapp.utils.deidentification.actions.deid_action import DeidentificationAction
from mainapp.utils.deidentification.common.enums import Actions


class FreeTextReplacement(DeidentificationAction):
    _ACTION_NAME = Actions.FREE_TEXT_REPLACEMENT.value

    def __init__(self, data_source, dsrc_method, col, lynx_type, mapping):
        super().__init__(data_source, dsrc_method, col, lynx_type)
        self.__mapping = mapping

    def update_mapping(self, new_values):
        self.__mapping.update(new_values)

    def _deid(self, value):
        deid_value = value[:]
        for original_text, deid_text in self.__mapping.items():
            deid_value = deid_value.lower().replace(original_text.lower(), deid_text)

        return deid_value
