from rest_framework.serializers import *
from mainapp.models import *


class ExecutionSerializer(ModelSerializer):
    class Meta:
        model = Execution
        fields = ('id', 'name', 'studies')


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
        fields = ('id','name','dir','s3_objects','type','about','programmatic_name','dataset','state','glue_table')
        extra_kwargs = {
            'state': {'read_only': True},
            'programmatic_name': {'read_only': True}
        }

class ActivitySerializer(ModelSerializer):

    class Meta:
        model = Activity
        fields = '__all__'
        extra_kwargs = {
            'user': {'read_only': True}
        }


class RequestSerializer(ModelSerializer):

    class Meta:
        model = Request
        fields = '__all__'

class DatasetSerializer(ModelSerializer):

    class Meta:
        model = Dataset
        fields = ('id', 'name', 'admin_users', 'aggregated_users', 'full_access_users','default_user_permission','user_created', 'tags', 'readme', 'description', 'updated_at', 'state','programmatic_name','bucket')

        extra_kwargs = {
            'tags': {'allow_empty': True},
            'admin_users': {'allow_empty': True},
            'aggregated_users': {'allow_empty': True},
            'full_access_users': {'allow_empty': True},
            'user_created': {'read_only': True},
            'bucket': {'read_only': True},
            'programmatic_name': {'read_only': True},
        }


class StudySerializer(ModelSerializer):
    # users = ListField(required=False, write_only=True)

    class Meta:
        model = Study
        fields = ('id','name',"datasets",'users','tags','updated_at','description','user_created')
        extra_kwargs = {
            'users': {'allow_empty': True}, #required = False?
            'datasets': {'allow_empty': True},
            'tags': {'allow_empty': True},
        }

class QuerySerializer(Serializer):
    query_string = CharField(max_length=2048)
    dataset_id = CharField(max_length=255)

class DatasetUploadedSerializer(Serializer):
    query = IntegerField()
    catalogs = ListField()
