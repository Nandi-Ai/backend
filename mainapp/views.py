# from rest_framework.generics import GenericAPIView
from rest_framework.views import APIView
from rest_framework.viewsets import ModelViewSet
from rest_framework.response import Response
from mainapp.models import Dataset, Execution,Study,User
from mainapp.serializers import DatasetSerializer,ExecutionSerializer
from rest_framework_swagger.views import get_swagger_view
from mainapp import settings
import boto3
#from botocore.vendored import requests #if using requests in a lambda fucntion
import requests
import uuid
from multiprocessing import Process
import time
import subprocess
import hashlib

schema_view = get_swagger_view(title='Lynx API')

class DatasetViewSet(ModelViewSet):
    def get_queryset(self):
        queryset=self.request.user.datasets

    serializer_class = DatasetSerializer
#
# class ExecutionManager(GenericAPIView):
#     serializer_class = ExecutionSerializer
#
#     def get(self, request):
#         try:
#             execution = Execution.objects.get(id=execution_id, hosital=request.user.hospital)
#
#         except Dataset.DoesNotExist:
#             return Response({"error": "dataset with that id not exists"}, status=400)
#
#         return Response(self.serializer_class(dataset, allow_null=True).data)

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
        p.start()  # TODO use kubernetes client for python instead

        # import kubernetes.config
        # from kubernetes.client.rest import ApiException
        # from pprint import pprint
        #
        # kubernetes.config.load_kube_config()
        # api_instance = kubernetes.client.CoreV1Api()
        # try:
        #     api_response = api_instance.connect_get_namespaced_pod_exec("jupyter-"+execution_identifier,namespace="jhub", command="python /usr/local/bin/sync_to_s3.py &")
        #     pprint(api_response)
        # except ApiException as e:
        #     print("Exception when calling CoreV1Api->connect_get_namespaced_pod_exec: %s\n" % e)

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

        # Create IAM client
        sts_default_provider_chain = boto3.client('sts', aws_access_key_id=settings.aws_access_key_id,
                                                        aws_secret_access_key=settings.aws_secret_access_key,
                                                        region_name=settings.aws_region_name)

        if service == "athena":
            role_to_assume_arn = 'arn:aws:iam::858916640373:role/athena_access'

        elif service == "s3":
            if permission =="read":
                role_to_assume_arn = 'arn:aws:iam::858916640373:role/s3readbucket'
            elif permission == "write":
                role_to_assume_arn = 'arn:aws:iam::858916640373:role/s3buckets2'
            else:
                return Response({"error":"must set permission to read or write"}, status=400)

        else:
            return Response({"error": "no service or service is not supported"}, status=400)

        role_session_name = 'session'

        response = sts_default_provider_chain.assume_role(
            RoleArn=role_to_assume_arn,
            RoleSessionName=role_session_name
        )

        try:
            study = Study.objects.get(execution = execution)
        except Study.DoesNotExist:
            return Response({"error": "this is not the execution of any study"}, 400)

        config = {}
        config['bucket'] = study.name+"-"+study.organization.name+"-lynx-workspace"
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
            User.objects.create_user(email=execution.identifier+"@lynx.md", password=hashlib.md5(execution.identifier.encode('utf-8')).hexdigest())
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

        return Response({'execution_identifier': study.execution.identifier, 'token': settings.jh_api_user_token}, status=201)
#
# class DatasetManager(GenericAPIView):
#     serializer_class = DatasetSerializer
#
#     def post(self, request):
#         scan_serialized = self.serializer_class(data=request.data)
#         if scan_serialized.is_valid():
#
#             scan, created = Dataset.objects.get_or_create(name=scan_serialized.validated_data['name'], user=request.user)
#             if not created:
#                 return Response({"error": "a scan with the same name is already exists"}, status=400)
#
#             scan.save()
#             #TODO upload files to s3 bucket for dataset
#             return Response(self.serializer_class(scan, allow_null=True).data, status=201)
#         else:
#             return Response({"error": scan_serialized.errors}, status=400)
#
#     def get(self, request, dataset_id):
#         try:
#             dataset = Dataset.objects.get(id=dataset_id, hosital = request.user.hospital)
#
#         except Dataset.DoesNotExist:
#             return Response({"error": "dataset with that id not exists"}, status=400)
#
#         return Response(self.serializer_class(dataset, allow_null=True).data)
