from rest_framework.generics import GenericAPIView
from rest_framework.views import APIView
from rest_framework.viewsets import ModelViewSet
from rest_framework.response import Response
from mainapp.models import Dataset, Execution,Study
from mainapp.serializers import DatasetSerializer,ExecutionSerializer
from rest_framework_swagger.views import get_swagger_view
from mainapp import settings
import boto3

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

        datasets = execution.study.datasets.all()

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

        creds = response['Credentials']

        config = {}
        config['buckets'] = [{"name":"lynx-dataset-bucket-"+str(dataset.id)} for dataset in datasets]
        config['aws_sts_creds'] = creds

        return Response(config)


class GetExecution(APIView):
    def get(self, request, study_id):
        from botocore.vendored import requests
        import uuid

        try:
            study = request.user.studies.get(id=study_id)
        except Study.DoesNotExist:
            return Response({"error":"study does not exists"}, 400)

        execution, created = Execution.objects.get_or_create(study = study)

        if created:
            execution.identifier = uuid.uuid4().hex
            execution.study = study
            execution.user = request.user
            execution.save()

            headers = {"Authorization": "Bearer " + settings.jh_api_admin_token}

            data = {
                "usernames": [
                    execution.identifier
                ],
                "admin": False
            }

            res = requests.post(settings.jh_url + "/hub/api/users", json=data, headers=headers)
            if res.status_code != 201:
                return Response({"error":"error creating execution"+str(res.text)})

        return Response({'execution_identifier': execution.identifier, 'token': settings.jh_api_user_token}, status=201)
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
