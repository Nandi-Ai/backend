from rest_framework.generics import GenericAPIView, CreateAPIView
from rest_framework.views import APIView
from rest_framework.viewsets import ModelViewSet, ReadOnlyModelViewSet
from rest_framework.response import Response
from mainapp.models import *
from mainapp.serializers import *
from rest_framework_swagger.views import get_swagger_view
from mainapp import settings
import boto3
#from botocore.vendored import requests #if using requests in a lambda fucntion
import requests
import uuid
from multiprocessing import Process
import time
import subprocess
from django.db.utils import IntegrityError
import json
from mainapp.lib import validate_query

schema_view = get_swagger_view(title='Lynx API')

class SendSyncSignal(APIView):
    def get(self, request):
        def send_sync_signal(ei):

            time.sleep(60)
            command = "kubectl exec jupyter-" + ei + " -- python /usr/local/bin/sync_to_s3.py &"
            subprocess.check_output(command.split(" "))

        ei = request.user.email.split('@')[0]
        try:
            execution = Execution.objects.get(identifier=ei)
        except Execution.DoesNotExist:
            return Response({"error": "execution does not exists"}, 400)

        p = Process(target=send_sync_signal, args=(execution.identifier,))
        p.start()

        return Response()

class GetSTS(APIView):
    def get(self, request):

        #execution_identifier = request.query_params.get('execution_identifier')
        ei = request.user.email.split('@')[0]
        permission = request.query_params.get('permission')
        service = request.query_params.get('service')

        try:
            execution = Execution.objects.get(identifier=ei)
        except Execution.DoesNotExist:
            return Response({"error": "execution does not exists"}, 400)

        try:
            study = Study.objects.get(execution = execution)
        except Study.DoesNotExist:
            return Response({"error": "this is not the execution of any study"}, 400)

        # Create IAM client
        sts_default_provider_chain = boto3.client('sts')

        workspace_bucket_name = "lynx-workspace-" + str(study.id)

        if service == "athena":
            role_to_assume_arn = 'arn:aws:iam::858916640373:role/athena_access'

        elif service == "s3":
            role_name = "lynx-workspace-" + str(study.id)
            if permission =="read":
                #role_to_assume_arn = 'arn:aws:iam::858916640373:role/s3readbucket'
                role_to_assume_arn = 'arn:aws:iam::858916640373:role/' + role_name
            elif permission == "write":
                # role_to_assume_arn = 'arn:aws:iam::858916640373:role/s3buckets2'
                role_to_assume_arn = 'arn:aws:iam::858916640373:role/' + role_name

            else:
                return Response({"error":"must set permission to read or write"}, status=400)

        else:
            return Response({"error": "no service or service is not supported"}, status=400)

        response = sts_default_provider_chain.assume_role(
            RoleArn=role_to_assume_arn,
            RoleSessionName='session'
        )

        config = {}
        config['bucket'] = workspace_bucket_name
        config['aws_sts_creds'] = response['Credentials']

        return Response(config)

class Dummy(APIView):
    def get(self, request):
        return Response()

class GetExecution(APIView):
    def get(self, request):
        study_id = request.query_params.get('study')

        try:
            study = request.user.studies.get(id=study_id)
        except Study.DoesNotExist:
            return Response({"error":"study does not exists"}, 400)

        if not study.execution:
            execution = Execution.objects.create()
            execution.identifier = uuid.uuid4().hex
            # execution.study = study
            execution.user = request.user
            execution.save()
            u = User.objects.create_user(email=execution.identifier+"@lynx.md", password=execution.identifier)
            u.is_execution = True
            u.save()
            study.execution = execution
            study.save()

            headers = {"Authorization": "Bearer " + settings.jh_api_admin_token}

            data = {
                "usernames": [
                    execution.identifier
                ],
                "admin": False
            }

            res = requests.post(settings.jh_url + "/hub/api/users", json=data, headers=headers)
            if res.status_code != 201:
                return Response({"error":"error creating execution: "+str(res.text)})

        return Response({'execution_identifier': study.execution.identifier, 'token': settings.jh_api_user_token})



class StudyViewSet(ReadOnlyModelViewSet):
    http_method_names = ['get', 'head', 'post','put']

    serializer_class = StudySerializer

    def get_queryset(self, **kwargs):
        return self.request.user.studies.all()

    def create(self, request, **kwargs):
        study_serialized = self.serializer_class(data=request.data)
        if study_serialized.is_valid():

            req_datasets = study_serialized.validated_data['datasets']
            study_name = study_serialized.validated_data['name']

            # TODO need to decide what to do with repeated datasets names: for example - if user A shared a dataset with user B ant the former has a dataset with the same name
            if study_name in [x.name for x in request.user.studies.all()]:
                return Response({"error": "this dataset already exist for that user"}, status=400)

            if not all(rds in request.user.datasets.all() for rds in req_datasets):
                return Response({"error": "not all datasets are related to the current user"}, status=400)

            study = Study.objects.create(name = study_name)

            req_users = study_serialized.validated_data['users']
            study.datasets.set(Dataset.objects.filter(id__in = [x.id for x in req_datasets]))
            study.users.set([request.user] + list(User.objects.filter(id__in = [x.id for x in req_users]))) #can user add also..
            study.user_created = request.user
            study.save()
            workspace_bucket_name ="lynx-workspace-"+ str(study.id)
            s3 = boto3.client('s3')
            s3.create_bucket(Bucket=workspace_bucket_name, CreateBucketConfiguration={'LocationConstraint':settings.aws_region},)
            with open('mainapp/s3_base_policy.json') as f:
                policy_json = json.load(f)
            policy_json['Statement'][0]['Resource'].append('arn:aws:s3:::'+workspace_bucket_name+'*')
            client = boto3.client('iam')
            policy_name = "lynx-workspace-"+ str(study.id)

            response = client.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_json)
            )

            policy_arn = response['Policy']['Arn']

            print(response)
            with open('mainapp/trust_relationship_doc.json') as f:
                trust_relationship_doc = json.load(f)

            role_name = "lynx-workspace-"+ str(study.id)
            client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_relationship_doc),
                Description=policy_name
            )

            response = client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy_arn
            )

            return Response(self.serializer_class(study, allow_null=True).data, status=201) #llow null, read_only, many
        else:
            return Response({"error": study_serialized.errors}, status=400)

    def update(self, request, *args, **kwargs):
        serialized = self.serializer_class(data=request.data, allow_null=True)
        study_name = serialized.validated_data['name']
        req_datasets = serialized.validated_data['datasets']

        if serialized.is_valid(): #if not valid super will handle it
            if study_name in [x.name for x in request.user.studies.all()]:
                return Response({"error": "this dataset already exist for that user"}, status=400)
            if not all(rds in request.user.datasets.all() for rds in req_datasets):
                return Response({"error": "not all datasets are related to the current user"}, status=400)

        return super(self.__class__, self).update(request=self.request)


class GetDatasetSTS(APIView):

    def get(self, request, dataset_id):
        try:
            dataset = request.user.datasets.get(id=dataset_id)

        except Dataset.DoesNotExist:
            return Response({"error": "dataset with that id not exists"}, status=400)

        # generate sts token so the user can upload the dataset to the bucket
        sts_default_provider_chain = boto3.client('sts')

        role_name = "lynx-dataset-"+str(dataset.id)
        role_to_assume_arn = 'arn:aws:iam::858916640373:role/' + role_name

        sts_response = sts_default_provider_chain.assume_role(
            RoleArn=role_to_assume_arn,
            RoleSessionName='session',
            DurationSeconds=43200
        )

        dataset_bucket_name = 'lynx-dataset-' + str(dataset.id)

        config = {}

        config['bucket'] = dataset_bucket_name
        config['aws_sts_creds'] = sts_response['Credentials']

        return Response(config)

class CurrentUserView(APIView):
    def get(self, request):
        serializer = UserSerializer(request.user)
        return Response(serializer.data)

class UserViewSet(ReadOnlyModelViewSet):
    # def get_queryset(self):
    #     return User.objects.filter(patient__in = self.request.user.related_patients).order_by("-created_at") #TODO check if it is needed to consider other doctors that gave a patient recommendation in generate recommendation.
    serializer_class = UserSerializer
    queryset = User.objects.filter(is_execution = False)


class TagViewSet(ReadOnlyModelViewSet):
    # def get_queryset(self):
    #     return User.objects.filter(patient__in = self.request.user.related_patients).order_by("-created_at") #TODO check if it is needed to consider other doctors that gave a patient recommendation in generate recommendation.
    serializer_class = TagSerializer
    queryset = Tag.objects.all()


class DatasetViewSet(ModelViewSet):
    http_method_names = ['get', 'head', 'post','put']

    def get_queryset(self):
        return self.request.user.datasets.all()
    # def get_queryset(self):
    #     return User.objects.filter(patient__in = self.request.user.related_patients).order_by("-created_at") #TODO check if it is needed to consider other doctors that gave a patient recommendation in generate recommendation.
    serializer_class = DatasetSerializer

    def create(self, request, **kwargs):
        dataset_serialized = self.serializer_class(data=request.data, allow_null=True)

        if dataset_serialized.is_valid():
            # create the dataset insance:
            dataset_name = dataset_serialized.validated_data['name']

            #TODO need to decide what to do with repeated datasets names: for example - if user A shared a dataset with user B ant the former has a dataset with the same name
            if dataset_name in [x.name for x in request.user.datasets.all()]:
                return Response({"error": "this dataset name already exist for that user"}, status=400)

            dataset = Dataset.objects.create(name = dataset_name)
            dataset.description = dataset_serialized.validated_data['description']
            dataset.readme = dataset_serialized.validated_data['readme']
            req_users = dataset_serialized.validated_data['users']
            dataset.users.set(
                [request.user] + list(User.objects.filter(id__in=[x.id for x in req_users])))  # can user add also..

            req_tags = dataset_serialized.validated_data['tags']
            dataset.tags.set(Tag.objects.filter(id__in=[x.id for x in req_tags]))
            dataset.user_created = request.user
            dataset.state = "private"

            dataset.save()

            dataset_bucket_name = 'lynx-dataset-'+str(dataset.id)

            # create the dataset bucket:
            s3 = boto3.client('s3')

            s3.create_bucket(Bucket=dataset_bucket_name,
                             CreateBucketConfiguration={'LocationConstraint': settings.aws_region}, )

            # create the dataset policy:
            with open('mainapp/s3_base_policy.json') as f:
                policy_json = json.load(f)
            policy_json['Statement'][0]['Resource'].append('arn:aws:s3:::' + dataset_bucket_name + '*')
            client = boto3.client('iam')

            policy_name = 'lynx-dataset-' + str(dataset.id)

            response = client.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_json)
            )

            policy_arn = response['Policy']['Arn']

            with open('mainapp/trust_relationship_doc.json') as f:
                trust_relationship_doc = json.load(f)

            # create the dataset role:
            role_name = "lynx-dataset-" + str(dataset.id)
            client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_relationship_doc),
                Description=policy_name,
                MaxSessionDuration=43200
            )

            # attach policy to role:
            response = client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy_arn
            )

            # generate sts token so the user can upload the dataset to the bucket
            sts_default_provider_chain = boto3.client('sts')

            role_to_assume_arn = 'arn:aws:iam::858916640373:role/' + role_name

            time.sleep(8)  # the role takes this time to be created!

            sts_response = sts_default_provider_chain.assume_role(
                RoleArn=role_to_assume_arn,
                RoleSessionName='session',
                DurationSeconds=43200
            )

            config = {}
            config['bucket'] = dataset_bucket_name
            config['aws_sts_creds'] = sts_response['Credentials']

            data = self.serializer_class(dataset, allow_null=True).data

            # add the sts token and bucket to the dataset response:
            data['config'] = config

            return Response(data, status=201)
            # TODO the frontend needs to notify when done uploading (in another method), and then needs to create a glue database to that dataset
        else:
            return Response({"error": dataset_serialized.errors}, status=400)

    def update(self, request, *args, **kwargs):
        dataset_serialized = self.serializer_class(data=request.data, allow_null=True)
        if dataset_serialized.is_valid(): #if not valid super will handle it

            dataset_name = dataset_serialized.validated_data['name']
            if dataset_name in [x.name for x in request.user.datasets.all()]:
                return Response({"error": "this dataset name already exist for that user"}, status=400)

        return super(self.__class__, self).update(request=self.request)

class DataSourceViewSet(ModelViewSet):
    serializer_class = DataSourceSerializer
    http_method_names = ['get', 'head', 'post','put']
    filter_fields = ('dataset',)

    def get_queryset(self):
        data_sources = DataSource.objects.none()

        for dataset in self.request.user.datasets.all():
            data_sources = data_sources | dataset.data_sources.all()
        return data_sources

    def create(self, request, *args, **kwargs):
        data_source_serialized = self.serializer_class(data=request.data, allow_null=True)
        if data_source_serialized.is_valid():
            dataset = data_source_serialized.validated_data['dataset']

            if dataset not in request.user.datasets.all():
                return Response({"error": "dataset doesn't exist or doesn't belong to the user"}, status=400)

            data_source = DataSource.objects.create(name = data_source_serialized['name'], dataset=dataset)
            return Response(self.serializer_class(data_source, allow_null=True).data, status=201)
        else:
            return Response({"error": data_source_serialized.errors}, status=400)

    def update(self, request, *args, **kwargs):
        serialized = self.serializer_class(data=request.data, allow_null=True)

        if serialized.is_valid(): #if not valid super will handle it
            dataset = serialized.validated_data['dataset']
            if dataset not in request.user.datasets.all():
                return Response({"error": "dataset doesn't exist or doesn't belong to the user"}, status=400)

        return super(self.__class__, self).update(request=self.request)


class RunQuery(GenericAPIView):
    serializer_class = QuerySerializer

    def post(self, request):
        query_serialized = self.serializer_class(data=request.data)
        if query_serialized.is_valid():
            ei = request.user.email.split('@')[0]

            try:
                execution = Execution.objects.get(identifier=ei)
            except Execution.DoesNotExist:
                return Response({"error": "execution does not exists"}, 400)

            try:
                study = Study.objects.get(execution=execution)
            except Study.DoesNotExist:
                return Response({"error": "this is not the execution of any study"}, 400)

            req_dataset_name = query_serialized.validated_data['dataset']

            try:
                dataset = study.datasets.get(name = req_dataset_name) #TODO need to make sure name of dataset is unique in study
            except Dataset.DoesNotExist:
                return Response({"error": "no permission to this dataset. make sure it is exists, yours or shared with you, and under that study"}, 400)


            client = boto3.client('athena')

            query = query_serialized.validated_data['query']

            validated, reason = validate_query(query = query, dataset = dataset)

            if not validated:
                return Response({"error": reason}, 400)

            response = client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    'Database': dataset.name #the name of the database in glue/athena
                },
                ResultConfiguration={
                    'OutputLocation': "s3://lynx-workspace-"+study.name+"-"+str(study.id),
                }
            )
            print(response)

            return Response({"query_execution_id": response['QueryExecutionId']})
