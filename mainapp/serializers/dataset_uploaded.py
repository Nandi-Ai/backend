from rest_framework.serializers import Serializer, IntegerField, ListField


class DatasetUploadedSerializer(Serializer):
    query = IntegerField()
    catalogs = ListField()
