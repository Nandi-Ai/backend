from rest_framework.serializers import Serializer, CharField, IntegerField


class CohortSerializer(Serializer):
    filter = CharField(max_length=2048, required=False)
    columns = CharField(max_length=2048, required=False)
    limit = IntegerField(required=False, default=None)
    dataset_id = CharField(max_length=255)
    data_source_id = CharField(max_length=255)
    destination_dataset_id = CharField(max_length=255)
