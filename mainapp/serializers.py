from rest_framework.serializers import *
from mainapp.models import Dataset, Execution, Study

class DatasetSerializer(ModelSerializer):
    class Meta:
        model = Dataset
        fields = ('name',)

class ExecutionSerializer(ModelSerializer):
    class Meta:
        model = Execution
        fields = ('name','studies')

class StudySerializer(ModelSerializer):

    class Meta:
        model = Study
        fields = ('name',"datasets")