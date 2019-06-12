from rest_framework.serializers import *
from mainapp.models import *


class ExecutionSerializer(ModelSerializer):
    class Meta:
        model = Execution
        fields = ('name','studies')

class UserSerializer(ModelSerializer):

    class Meta:
        model = User
        fields = ('email','organization','name')

class TagSerializer(ModelSerializer):

    class Meta:
        model = Tag
        fields = ('name', 'category',)

class DatasetSerializer(ModelSerializer):
    tags = TagSerializer(many=True, allow_null=True)
    users = UserSerializer(many=True, allow_null=True) #read_only = displaying only when GET and not POST
    class Meta:
        model = Dataset
        fields = ('name','users','tags','readme','description')

class StudySerializer(ModelSerializer):
    users = UserSerializer(many=True, allow_null=True)
    datasets =  DatasetSerializer(many=True, allow_null=True) #TODO can a study have no datasets??
    class Meta:
        model = Study
        fields = ('name',"datasets",'users')

class QuerySerializer(Serializer):
    query = CharField(max_length=2048)
    dataset = CharField(max_length=255)