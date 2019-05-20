from rest_framework.serializers import *
from mainapp.models import Dataset

class DatasetSerializer(ModelSerializer):
    class Meta:
        model = Dataset
        fields = ('id', 'name')
