import logging

from botocore.config import Config
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

from mainapp.models import Documentation
from mainapp.serializers import DocumentationSerializer
from mainapp.utils import aws_service

logger = logging.getLogger(__name__)


class DocumentationViewSet(ModelViewSet):
    http_method_names = ["get", "head", "post", "put", "delete"]
    serializer_class = DocumentationSerializer

    def get_serializer(self, *args, **kwargs):
        if "data" in kwargs:
            data = kwargs["data"]

            # check if many is required
            if isinstance(data, list):
                kwargs["many"] = True
        return super(DocumentationViewSet, self).get_serializer(*args, **kwargs)

    def get_queryset(self):
        return self.get_documentation_obj()

    def get_documentation_obj(self):
        if len(self.request.query_params) > 0:
            dataset_id = self.request.query_params["dataset"]
            return Documentation.objects.filter(dataset_id=dataset_id)
        else:
            documentation_id = self.request.parser_context["kwargs"]["pk"]
            return Documentation.objects.filter(id=documentation_id)

    @action(detail=True, methods=["get"])
    def signed_url(self, request, pk=None):
        documentation = self.get_object()
        dataset = documentation.dataset
        org_name = dataset.organization.name
        s3_client = aws_service.create_s3_client(
            org_name=org_name,
            config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
        )
        url = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": dataset.bucket,
                "Key": f"documentation/{documentation.file_name}",
            },
            ExpiresIn=30,
        )
        return Response(url)
