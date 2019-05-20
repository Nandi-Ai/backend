from rest_framework.generics import GenericAPIView
from rest_framework.views import APIView
from rest_framework.viewsets import ModelViewSet
from rest_framework.response import Response
from mainapp.models import Dataset, User
from mainapp.serializers import DatasetSerializer
from rest_framework_swagger.views import get_swagger_view

schema_view = get_swagger_view(title='lynx API')

#
# class DatasetViewSet(ModelViewSet):
#
#     def get_queryset(request):
#         queryset=
#     queryset = User.hospital.datasets.all()
#     serializer_class = DatasetSerializer

class DatasetManager(GenericAPIView):
    serializer_class = DatasetSerializer

    def post(self, request):
        scan_serialized = self.serializer_class(data=request.data)
        if scan_serialized.is_valid():

            scan, created = Dataset.objects.get_or_create(name=scan_serialized.validated_data['name'], user=request.user)
            if not created:
                return Response({"error": "a scan with the same name is already exists"}, status=400)

            scan.save()
            #TODO upload files to s3 bucket for dataset
            return Response(self.serializer_class(scan, allow_null=True).data, status=201)
        else:
            return Response({"error": scan_serialized.errors}, status=400)

    def get(self, request, dataset_id):
        try:
            dataset = Dataset.objects.get(id=dataset_id, hosital = request.user.hospital)

        except Dataset.DoesNotExist:
            return Response({"error": "dataset with that id not exists"}, status=400)

        return Response(self.serializer_class(dataset, allow_null=True).data)
