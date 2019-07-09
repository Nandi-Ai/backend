from rest_framework.serializers import *
from mainapp.models import *


class ExecutionSerializer(ModelSerializer):
    class Meta:
        model = Execution
        fields = ('id', 'identifier', 'name', 'studies')


class UserSerializer(ModelSerializer):

    class Meta:
        model = User
        fields = ('id', 'email', 'organization', 'name')

class TagSerializer(ModelSerializer):

    class Meta:
        model = Tag
        fields = ('id', 'name', 'category',)


class DataSourceSerializer(ModelSerializer):

    class Meta:
        model = DataSource
        fields = '__all__'

class DatasetSerializer(ModelSerializer):

    class Meta:
        model = Dataset
        fields = ('id','name','users', 'tags','readme','description','updated_at','state')
        extra_kwargs = {
            'tags': {'allow_empty': True},
            'users': {'allow_empty': True},
        }


class StudySerializer(ModelSerializer):
    # users = ListField(required=False, write_only=True)

    class Meta:
        model = Study
        fields = ('id','name',"datasets",'users','tags','updated_at','description')
        extra_kwargs = {
            'users': {'allow_empty': True}, #required = False?
            'datasets': {'allow_empty': True},
            'tags': {'allow_empty': True},
        }

class QuerySerializer(Serializer):
    query = CharField(max_length=2048)
    dataset = CharField(max_length=255)

class DatasetUploadedSerializer(Serializer):
    query = IntegerField()
    catalogs = ListField()
