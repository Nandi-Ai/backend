from rest_framework.generics import GenericAPIView
from rest_framework.views import APIView
from rest_framework.viewsets import ModelViewSet
from rest_framework.response import Response
from mainapp.models import Dataset, Execution,Study
from mainapp.serializers import DatasetSerializer,ExecutionSerializer
from rest_framework_swagger.views import get_swagger_view
from mainapp import settings
import boto3
#from botocore.vendored import requests #if using requests in a lambda fucntion
import requests
import uuid

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

class GetExecutionConfig(APIView):
    def get(self, request, execution_identifier):
        try:
            execution = Execution.objects.get(identifier=execution_identifier)
        except Execution.DoesNotExist:
            return Response({"error": "execution does not exists"}, 400)

        # Create IAM client
        sts_default_provider_chain = boto3.client('sts', aws_access_key_id=settings.aws_access_key_id,
                                                        aws_secret_access_key=settings.aws_secret_access_key,
                                                        region_name=settings.aws_region_name)

        print('Default Provider Identity: : ' + sts_default_provider_chain.get_caller_identity()['Arn'])

        role_to_assume_arn = 'arn:aws:iam::858916640373:role/s3buckets2'
        role_session_name = 'test_session'

        response = sts_default_provider_chain.assume_role(
            RoleArn=role_to_assume_arn,
            RoleSessionName=role_session_name
        )

        study = Study.objects.get(execution = execution)

        config = {}
        config['bucket'] = study.name+"-"+study.organization.name+"-lynx-workspace"
        config['aws_sts_creds'] = response['Credentials']


        def send_sync_signal(execution_identifier):
            import time
            import subprocess

            time.sleep(60)
            command = "kubectl exec jupyter-"+execution_identifier+" -- python /usr/local/bin/sync_to_s3.py &"
            subprocess.check_output(command.split(" "))

        from multiprocessing import Process
        p = Process(target=send_sync_signal, args=(execution_identifier,))
        p.start() #TODO use kubernetes client for python instead

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

        return Response(config)

class GetExecution(APIView):
    def get(self, request, study_id):

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
