from rest_framework.serializers import ModelSerializer

from mainapp.models import Documentation


class DocumentationSerializer(ModelSerializer):
    def __init__(self, *args, **kwargs):
        many = kwargs.pop("many", True)
        super(DocumentationSerializer, self).__init__(many=many, *args, **kwargs)

    class Meta:
        model = Documentation
        fields = ("id", "dataset", "file_name")
