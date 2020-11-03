from rest_framework.serializers import ChoiceField, Serializer
from mainapp.utils.lib import PrivilegePath


class ReadStsSerializer(Serializer):

    PERMISSION_CHOICES = (
        (PrivilegePath.FULL.value, PrivilegePath.FULL.value),
        (PrivilegePath.AGG_STATS.value, PrivilegePath.AGG_STATS.value),
        (PrivilegePath.LIMITED.value, PrivilegePath.LIMITED.value),
        (PrivilegePath.DEID.value, PrivilegePath.DEID.value),
    )

    permission = ChoiceField(choices=PERMISSION_CHOICES, required=False)
