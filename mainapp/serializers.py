from rest_framework.serializers import *
from mainapp.models import *


class ExecutionSerializer(ModelSerializer):
    class Meta:
        model = Execution
        fields = ('id','identifier','name','studies')

class UserSerializer(ModelSerializer):

    class Meta:
        model = User
        fields = ('id','email','organization','name')

class TagSerializer(ModelSerializer):

    class Meta:
        model = Tag
        fields = ('id','name', 'category',)

class DatasetSerializer(ModelSerializer):
    tags = TagSerializer(many=True, allow_null=True)
    users = UserSerializer(many=True, allow_null=True) #read_only = displaying only when GET and not POST

    class Meta:
        model = Dataset
        fields = ('id','name','users','tags','readme','description','updated_at','state')

class StudySerializer(ModelSerializer):
    users = UserSerializer(many=True, allow_null=True)
    datasets =  DatasetSerializer(many=True, allow_null=True) #TODO can a study have no datasets??
    class Meta:
        model = Study
        fields = ('id','name',"datasets",'users')

class QuerySerializer(Serializer):
    query = CharField(max_length=2048)
    dataset = CharField(max_length=255)