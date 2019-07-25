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
from mainapp import lib
import pyreadstat
import threading
import zipfile
import os
import shutil
import dateparser
from django.core import exceptions
# from rest_framework.filters import BaseFilterBackend
# import coreapi

schema_view = get_swagger_view(title='Lynx API')

class Error(Response):

    def __init__(self, error_text, status=400):
        super().__init__()
        self.status = status
        self.data = {"error":error_text}

class SendSyncSignal(APIView):
    def get(self, request):
        def send_sync_signal(ei):

            time.sleep(60)
            command = "kubectl exec jupyter-" + ei + " -- python /usr/local/bin/sync_to_s3.py &"
            subprocess.check_output(command.split(" "))

        ei = request.user.email.split('@')[0]
        try:
            execution = Execution.objects.get(id=ei)
        except Execution.DoesNotExist:
            return Error("execution does not exists")

        p = Process(target=send_sync_signal, args=(execution.id,))
        p.start()

        return Response()

class GetSTS(APIView):
    def get(self, request):

        #execution_identifier = request.query_params.get('execution_identifier')
        ei = request.user.email.split('@')[0]
        service = request.query_params.get('service')

        try:
            execution = Execution.objects.get(id=ei)
        except Execution.DoesNotExist:
            return Error("execution does not exists")

        try:
            study = Study.objects.get(execution = execution)
        except Study.DoesNotExist:
            return Error("this is not the execution of any study")

        # Create IAM client
        sts_default_provider_chain = boto3.client('sts')

        workspace_bucket_name = "lynx-workspace-" + str(study.id)

        if service == "athena":
            role_to_assume_arn = 'arn:aws:iam::858916640373:role/athena_access'

        elif service == "s3":
            role_name = "lynx-workspace-" + str(study.id)
            role_to_assume_arn = 'arn:aws:iam::858916640373:role/' + role_name

        else:
            return Error("please mention an aws service in a query string param")

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
        return Error("the error")

class GetExecution(APIView):
    def get(self, request):
        study_id = request.query_params.get('study')

        try:
            study = request.user.studies.get(id=study_id)
        except Study.DoesNotExist:
            return Error("study does not exists")

        if not study.execution:
            execution = Execution.objects.create()

            # execution.study = study
            execution.user = request.user
            execution.save()
            u = User.objects.create_user(email=str(execution.id)+"@lynx.md", password=execution.id)
            u.is_execution = True
            u.save()
            study.execution = execution
            study.save()

            headers = {"Authorization": "Bearer " + settings.jh_api_admin_token}

            data = {
                "usernames": [
                    str(execution.id)
                ],
                "admin": False
            }

            res = requests.post(settings.jh_url + "/hub/api/users", json=data, headers=headers)
            if res.status_code != 201:
                return Error("error creating execution")

        return Response({'execution_identifier': str(study.execution.id), 'token': settings.jh_api_user_token})


class StudyViewSet(ModelViewSet):
    http_method_names = ['get', 'head', 'post','put','delete']

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
                return Error("this dataset already exist for that user")

            if not all(rds in request.user.datasets.all() for rds in req_datasets):
                return Error("not all datasets are related to the current user")

            study = Study.objects.create(name = study_name)
            study.description = study_serialized.validated_data['description']
            req_users = study_serialized.validated_data['users']
            study.datasets.set(Dataset.objects.filter(id__in = [x.id for x in req_datasets]))
            study.users.set([request.user] + list(User.objects.filter(id__in = [x.id for x in req_users]))) #can user add also..
            study.user_created = request.user

            req_tags = study_serialized.validated_data['tags']
            study.tags.set(Tag.objects.filter(id__in=[x.id for x in req_tags]))

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
            return Error(study_serialized.errors)

    # def update(self, request, *args, **kwargs):
    #     serialized = self.serializer_class(data=request.data, allow_null=True)
    #     study_name = serialized.validated_data['name']
    #     req_datasets = serialized.validated_data['datasets']
    #
    #     if serialized.is_valid(): #if not valid super will handle it
    #         if study_name in [x.name for x in request.user.studies.all()]:
    #             return Response({"error": "this dataset already exist for that user"}, status=400)
    #         if not all(rds in request.user.datasets.all() for rds in req_datasets):
    #             return Response({"error": "not all datasets are related to the current user"}, status=400)
    #
    #     return super(self.__class__, self).update(request=self.request)


class GetDatasetSTS(APIView):

    def get(self, request, dataset_id):
        try:
            dataset = request.user.datasets.get(id=dataset_id)

        except Dataset.DoesNotExist:
            return Error("dataset with that id not exists")

        # generate sts token so the user can upload the dataset to the bucket
        sts_default_provider_chain = boto3.client('sts')

        role_name = "lynx-dataset-"+str(dataset.id)
        role_to_assume_arn = 'arn:aws:iam::858916640373:role/' + role_name

        sts_response = sts_default_provider_chain.assume_role(
            RoleArn=role_to_assume_arn,
            RoleSessionName='session',
            DurationSeconds=43200
        )


        config = {}

        config['bucket'] = dataset.bucket
        config['aws_sts_creds'] = sts_response['Credentials']

        return Response(config)


class HandleDatasetAccessRequest(APIView):
    def get(self, request, dataset_id):
        response = request.query_params.get('response')
        user_requested_id = request.query_params.get('user')
        if response not in ["approve", "deny"]:
            return Error("please provide a response as query string param - approve or deny")

        try:
            dataset = request.user.admin_datasets.get(id=dataset_id)
        except Dataset.DoesNotExist:
            return Error("dataset not exist or you don't admin it")

        try:
            user_requested = (dataset.users_requested_full_access.all() | dataset.users_requested_aggregated_access).extinct().get(id = user_requested_id)
        except User.DoesNotExist:
            return Error("the user does not exist, did not requested full access for that dataset or you didn't mention a user as a query string param")

        if user_requested in dataset.users_requested_full_access:
            dataset.users_requested_full_access.remove(user_requested)
            if response == "approve":
                dataset.users_requested_full_access.add(user_requested)

        if user_requested in dataset.users_requested_aggregated_access:
            dataset.users_requested_aggregated_access.remove(user_requested)
            if response == "approve":
                dataset.users_requested_aggregated_access.add(user_requested)

        return Response()


class GetDatasetAccessRequestList(APIView):
    def get(self, request):
        datasets = request.user.admin_datasets.all()
        requests  = []
        for dataset in datasets:
            for user in dataset.users_requested_aggregated_access.all():
                req = {}
                req['permission'] = "aggregated"
                req['user'] = UserSerializer(user).data
                req['dataset'] = DatasetSerializer(dataset).data
                requests.append(req)

            for user in dataset.users_requested_full_access.all():
                req = {}
                req['permission'] = "full"
                req['user'] = UserSerializer(user).data
                req['dataset'] = DatasetSerializer(dataset).data
                requests.append(req)

        return Response(requests)


class RequestAccessForDataset(APIView):
    def get(self, request, dataset_id):
        permission_request_types = ["aggregated, full"]
        requested_permission = request.query_params.get('permission')
        try:
            dataset = request.user.datasets.filter(state="private").get(id=dataset_id)
        except Dataset.DoesNotExist:
            return Error("can't request access for this dataset")

        if requested_permission not in permission_request_types.all():
            return Error("permission must be one of: "+str(permission_request_types))

        if request.user in dataset.full_access_users.all():
            return Error("you already have full access for this dataset")

        if request.user in dataset.aggregated_users.all() and requested_permission == "aggregated":
                return Error("you already have aggregated access for this dataset")

        # a user with aggregated access can request full access

        if request.user in dataset.users_requested_full_access.all():
            if requested_permission == "aggregated":
                return Error("you have full acess to that dataset so you don't need aggregated access")
            if requested_permission == "full":
                return Error("you have already requested full access for that dataset")

        if request.user in dataset.users_requested_aggregated_access.all():
            if requested_permission == "aggregated":
                return Error("you  already requested aggregated access for this dataset")
            if requested_permission == "full":
                return Error("you have already requested aggregated access for this dataset. you have to wait for an admin to response your current request before requesting full access")

        if requested_permission == "aggregated":
            dataset.users_requested_aggregated_access.add(request.user)
        if requested_permission == "full":
            dataset.users_requested_full_access.add(request.user)

        return Response()


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
    http_method_names = ['get', 'head', 'post', 'put', 'delete']

    def logic_validate(self, request, dataset_data): #only common validations for create and update! #

        if dataset_data['state'] == "private":
            if not 'default_user_permission' in dataset_data:
                return Error("default_user_permission must be set sice the state is private")

            if not dataset_data['default_user_permission']:
                return Error("default_user_permission must be none or aggregated")

    def get_queryset(self):
        return self.request.user.datasets.all()
    serializer_class = DatasetSerializer

    def create(self, request, **kwargs):
        dataset_serialized = self.serializer_class(data=request.data, allow_null=True)

        if dataset_serialized.is_valid():
            # create the dataset insance:
            #TODO maybe use super() as in update instead of completing the all process.

            dataset_data = dataset_serialized.validated_data

            #validations common for create and update:
            error_response = self.logic_validate(request, dataset_data)
            if error_response:
                return error_response

            #additional validation only for create:
            if dataset_data['state'] == "public" and dataset_data['aggregated_users']:
                return Error("dataset with public state can't have aggregated users")

            if dataset_data['state'] == "archived":
                return Error("can't create new dataset with status archived")

            dataset = Dataset.objects.create(name = dataset_data['name'])

            dataset.description = dataset_data['description']
            dataset.readme = dataset_data['readme']
            req_admin_users = dataset_data['admin_users']
            dataset.admin_users.set([request.user] + list(User.objects.filter(id__in=[x.id for x in req_admin_users])))
            req_aggregated_users = dataset_data['aggregated_users']
            dataset.aggregated_users.set(list(User.objects.filter(id__in=[x.id for x in req_aggregated_users])))
            req_full_access_users = dataset_data['full_access_users']
            dataset.full_access_users.set(list(User.objects.filter(id__in=[x.id for x in req_full_access_users])))
            dataset.state = dataset_data['state']
            dataset.default_user_permission = dataset_data['default_user_permission']
            req_tags = dataset_data['tags']
            dataset.tags.set(Tag.objects.filter(id__in=[x.id for x in req_tags]))
            dataset.user_created = request.user
            dataset.bucket = 'lynx-dataset-' + str(dataset.id)
            dataset.save()

            # create the dataset bucket:
            s3 = boto3.client('s3')
            s3.create_bucket(Bucket=dataset.bucket,
                             CreateBucketConfiguration={'LocationConstraint': settings.aws_region}, )

            cors_configuration = {
                'CORSRules': [{
                    'AllowedHeaders': ['*'],
                    'AllowedMethods': ['GET', 'PUT', 'POST', 'DELETE'],
                    'AllowedOrigins': ['*'],
                    'MaxAgeSeconds': 3000
                }]
            }

            s3.put_bucket_cors(Bucket=dataset.bucket, CORSConfiguration=cors_configuration)

            # create the dataset policy:
            with open('mainapp/s3_base_policy.json') as f:
                policy_json = json.load(f)
            policy_json['Statement'][0]['Resource'].append('arn:aws:s3:::' + dataset.bucket + '*')
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

            client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy_arn
            )

            time.sleep(8)  # the role takes this time to be created! it is here in order to prevent calling GetDatasetSTS before creation
            data = self.serializer_class(dataset, allow_null=True).data

            return Response(data, status=201)
        else:
            return Error(dataset_serialized.errors)

    def update(self, request, *args, **kwargs):
        dataset_serialized = self.serializer_class(data=request.data, allow_null=True)

        if dataset_serialized.is_valid():
            dataset_data = dataset_serialized.validated_data

            error_response = self.logic_validate(request, dataset_data)
            if error_response:
                return error_response

            #additional validations only for update:

            dataset = Dataset.objects.get(id=self.request._data['id'])

            if request.user not in dataset.admin_users.all():
                return Error("this user can't update the dataset")

        # return super(self.__class__, self).update(request=self.request) #will handle the case where serializer is not valid
    #
    # def partial_update(self, request, *args, **kwargs):
    #     dataset_serialized = self.serializer_class(data=request.data, allow_null=True,partial=True)


class DataSourceViewSet(ModelViewSet):
    serializer_class = DataSourceSerializer
    http_method_names = ['get', 'head', 'post','put','delete']
    filter_fields = ('dataset',)

    def get_queryset(self):
        return self.request.user.data_sources

    def create(self, request, *args, **kwargs):
        ds_types = ['structured', 'images', 'zip']
        data_source_serialized = self.serializer_class(data=request.data, allow_null=True)

        if data_source_serialized.is_valid():
            data_source_data= data_source_serialized.validated_data
            dataset = data_source_data['dataset']

            if dataset not in request.user.datasets.all():
                return Error("dataset doesn't exist or doesn't belong to the user")

            if data_source_data['type'] not in ds_types:
                return Error("data source type must be one of: " + str(ds_types))

            if not isinstance(data_source_data['s3_objects'], list):
                return Error("s3 objects must be a (json) list")

            if data_source_data['type'] in ["zip", "structured"] and len(data_source_data['s3_objects']) > 1:
                return Error("data source of type structured and zip must include exactly one item in s3_objects json array")

            data_source = data_source_serialized.save()

            if data_source.type == "structured":
                s3_obj = data_source.s3_objects[0]
                path, file_name, file_name_no_ext, ext = lib.break_s3_object(s3_obj)

                if ext in ["sav", "zsav", "csv"]:

                    if ext in ["sav", "zsav"]: #convert to csv
                        s3_client = boto3.client('s3')
                        workdir = "/tmp/" + str(data_source.id)
                        os.makedirs(workdir)
                        s3_client.download_file(data_source.dataset.bucket, s3_obj, workdir +"/"+ file_name)
                        df, meta = pyreadstat.read_sav(workdir +"/"+ file_name)
                        csv_path_and_file = workdir+"/" + file_name_no_ext + ".csv"
                        df.to_csv(csv_path_and_file)
                        s3_client.upload_file(csv_path_and_file, data_source.dataset.bucket,
                                              path + "/" + file_name_no_ext + ".csv")
                        data_source.s3_objects.pop()
                        data_source.s3_objects.append(path + "/" + file_name_no_ext + ".csv")
                        shutil.rmtree(workdir)

                    create_catalog_thread = threading.Thread(target=lib.create_catalog, args=[data_source])  # also setting the data_source state to ready when it's done
                    create_catalog_thread.start()

                else:
                    return Error("structured file type is not supported")

            elif data_source.type == "zip":
                handle_zip_thread = threading.Thread(target=lib.handle_zipped_data_source, args=[data_source])
                handle_zip_thread.start()

            else:
                data_source.state = "ready"

            data_source.save()
            return Response(self.serializer_class(data_source, allow_null=True).data, status=201)

        else:
            return Error(data_source_serialized.errors)

    def update(self, request, *args, **kwargs):
        serialized = self.serializer_class(data=request.data, allow_null=True)

        if serialized.is_valid(): #if not valid super will handle it
            dataset = serialized.validated_data['dataset']
            if dataset not in request.user.datasets.all():
                return Error("dataset doesn't exist or doesn't belong to the user")

        return super(self.__class__, self).update(request=self.request)


class RunQuery(GenericAPIView):
    serializer_class = QuerySerializer

    def post(self, request):
        query_serialized = self.serializer_class(data=request.data)
        if query_serialized.is_valid():
            ei = request.user.email.split('@')[0]

            try:
                execution = Execution.objects.get(id=ei)
            except Execution.DoesNotExist:
                return Error("execution does not exists")

            try:
                study = Study.objects.get(execution=execution)
            except Study.DoesNotExist:
                return Error("this is not the execution of any study")

            req_dataset_name=query_serialized.validated_data['dataset']

            try:
                dataset = study.datasets.get(name=req_dataset_name) #TODO need to make sure name of dataset is unique in study
            except Dataset.DoesNotExist:
                return Error("no permission to this dataset. make sure it is exists, yours or shared with you, and under that study")

            client = boto3.client('athena')

            query = query_serialized.validated_data['query']

            validated, reason = validate_query(query=query, dataset=dataset)

            if not validated:
                return Error(reason)

            response = client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    'Database': dataset.name  # the name of the database in glue/athena
                },
                ResultConfiguration={
                    'OutputLocation': "s3://lynx-workspace-"+study.name+"-"+str(study.id),
                }
            )

            return Response({"query_execution_id": response['QueryExecutionId']})


class ActivityViewSet(ModelViewSet):
    serializer_class = ActivitySerializer
    http_method_names = ['get', 'head', 'post','put','delete']
    filter_fields = ('user', 'dataset', 'study')
    #
    # class StartFilter(BaseFilterBackend):
    #     def get_schema_fields(self, view):
    #         return [coreapi.Field(name='start', location='query', required=True,
    #                               description="date in YYYY-MM-DD HH:MM[:ss[.uuuuuu]][TZ] format (time is optional)",
    #                               type='string')]
    #
    # class EndFilter(BaseFilterBackend):
    #     def get_schema_fields(self, view):
    #         return [coreapi.Field(name='start', location='query', required=True,
    #                               description="datetime in any format",
    #                               type='string')]

    # filter_backends = (StartFilter, EndFilter)

    def get_queryset(self):
        #all activity for all datasets that the user admins
        return Activity.objects.filter(dataset_id__in=[x.id for x in self.request.user.admin_datasets.all()])

    def list(self, request, *args, **kwargs):
        start_raw = request.GET.get('start')
        end_raw = request.GET.get('end')

        if not all([start_raw, end_raw]):
            return Error("please provide start and end as query string params in some datetime format")
        try:
            start = dateparser.parse(start_raw)
            end = dateparser.parse(end_raw)
        except exceptions.ValidationError as e:
            return Error("cannot parse this format: "+str(e))

        queryset = self.get_queryset().filter(ts__range = (start, end)).order_by("-ts")
        serializer = self.serializer_class(data=queryset,  allow_null = True, many=True)
        serializer.is_valid()

        return Response(serializer.data)


class GetExecutionConfig(APIView):
    def get(self, request):

        execution = Execution.objects.get(id = request.user.email.split("@")[0])
        real_user = execution.user

        config = {}
        config['datasets'] = DatasetSerializer(real_user.datasets, many=True)
        return config
