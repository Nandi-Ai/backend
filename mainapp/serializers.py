from rest_framework.serializers import *

from mainapp.models import (
    Organization,
    Execution,
    User,
    Documentation,
    Tag,
    DataSource,
    Activity,
    Study,
    Request,
    Dataset
)


class ExecutionSerializer(ModelSerializer):
    class Meta:
        model = Execution
        fields = ('id', 'name', 'studies')


class UserSerializer(ModelSerializer):
    class Meta:
        model = User
        fields = ('id', 'email', 'organization', 'name')


class OrganizationSerializer(ModelSerializer):
    class Meta:
        model = Organization
        fields = ('id', 'name', 'logo',)


class DocumentationSerializer(ModelSerializer):
    def __init__(self, *args, **kwargs):
        many = kwargs.pop('many', True)
        super(DocumentationSerializer, self).__init__(many=many, *args, **kwargs)

    class Meta:
        model = Documentation
        fields = ('id', 'dataset', 'file_name')


class TagSerializer(ModelSerializer):
    class Meta:
        model = Tag
        fields = ('id', 'name', 'category',)


class DataSourceSerializer(ModelSerializer):
    class Meta:
        model = DataSource
        fields = (
            'id', 'name', 'dir', 's3_objects', 'type', 'about', 'programmatic_name', 'dataset', 'state', 'glue_table',
            'children', 'ancestor', 'cohort')
        extra_kwargs = {
            'state': {'read_only': True},
            'cohort': {'read_only': True},
            'programmatic_name': {'read_only': True},
            'children': {'read_only': True}
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
        fields = (
            'id',
            'name',
            'admin_users',
            'aggregated_users',
            'full_access_users',
            'is_discoverable',
            'default_user_permission',
            'user_created',
            'updated_at',
            'created_at',
            'tags',
            'readme',
            'description',
            'organization',
            'state',
            'programmatic_name',
            'bucket',
            'cover',
            'children',
            'ancestor',
        )

        extra_kwargs = {
            'children': {'read_only': True},
            'tags': {'allow_empty': True},
            'admin_users': {'allow_empty': True},
            'aggregated_users': {'allow_empty': True},
            'full_access_users': {'allow_empty': True},
            'user_created': {'read_only': True},
            'bucket': {'read_only': True},
            'programmatic_name': {'read_only': True},
            'updated_at': {'read_only': True},
            'created_at': {'read_only': True}
        }


class StudySerializer(ModelSerializer):
    # users = ListField(required=False, write_only=True)

    class Meta:
        model = Study
        fields = (
            'id',
            'name',
            'datasets',
            'users',
            'tags',
            'updated_at',
            'description',
            'user_created',
            'cover',
        )
        extra_kwargs = {
            'users': {'allow_empty': True},  # required = False?
            'datasets': {'allow_empty': True},
            'tags': {'allow_empty': True},
        }


class SimpleQuerySerializer(Serializer):
    query_string = CharField(max_length=2048)
    dataset_id = CharField(max_length=255)


class QuerySerializer(Serializer):
    query = CharField(max_length=2048, required=False, default=None)
    filter = CharField(max_length=2048, required=False)
    columns = CharField(max_length=2048, required=False)
    limit = IntegerField(required=False, default=None)
    sample_aprx = IntegerField(required=False, default=None)
    dataset_id = CharField(max_length=255)
    data_source_id = CharField(max_length=255)


class CohortSerializer(Serializer):
    filter = CharField(max_length=2048, required=False)
    columns = CharField(max_length=2048, required=False)
    limit = IntegerField(required=False, default=None)
    dataset_id = CharField(max_length=255)
    data_source_id = CharField(max_length=255)
    destination_dataset_id = CharField(max_length=255)


class DatasetUploadedSerializer(Serializer):
    query = IntegerField()
    catalogs = ListField()
