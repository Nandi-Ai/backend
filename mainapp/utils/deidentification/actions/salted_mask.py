import hashlib
import logging

from mainapp.utils.deidentification.actions.mask import Mask
from mainapp.utils.deidentification.common.enums import Actions

logger = logging.getLogger(__name__)


class SaltedMask(Mask):
    _ACTION_NAME = Actions.SALTED_HASH.value

    def __init__(self, data_source, dsrc_method, col, lynx_type):
        super(Mask, self).__init__(data_source, dsrc_method, col, lynx_type)
        self._salt_key = self._dsrc_method.method.salt_key.hex

    def _deid(self, value):
        masked_value = hashlib.sha3_224(
            value.encode("utf-8") + self._salt_key.encode("utf-8")
        ).hexdigest()
        return self._mask(masked_value)
