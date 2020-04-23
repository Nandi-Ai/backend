from rest_framework.serializers import Serializer, CharField


class SimpleQuerySerializer(Serializer):
    query_string = CharField(max_length=2048)
    dataset_id = CharField(max_length=255)
