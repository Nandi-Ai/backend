import os
import logging

from botocore.config import Config
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

from mainapp.models import Documentation
from mainapp.serializers import DocumentationSerializer
from mainapp.utils import aws_service, lib
from mainapp.utils.response_handler import BadRequestErrorResponse

logger = logging.getLogger(__name__)


class DocumentationViewSet(ModelViewSet):
    http_method_names = ["get", "head", "post", "put", "delete"]
    serializer_class = DocumentationSerializer
    file_types = {
        ".jpg": ["image/jpeg"],
        ".jpeg": ["image/jpeg"],
        ".tiff": ["image/tiff"],
        ".png": ["image/png"],
        ".csv": ["application/csv", "text/csv", "text/plain"],
        ".sav": ["application/octet-stream"],
        ".zsav": [],
        ".doc": ["application/msword"],
        ".pdf": ["application/pdf"],
        ".zip": ["application/zip"],
    }

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

    def create(self, request, *args, **kwargs):
        doc_serialized = self.get_serializer(data=request.data)
        try:
            doc_serialized.is_valid(raise_exception=True)
            documentations = doc_serialized.save()
            dataset = documentations[0].dataset
            workdir = f"/tmp/documentation_{dataset.id}"
            s3_client = aws_service.create_s3_client(org_name=dataset.organization.name)
            validated_files = []
            try:
                for documentation in documentations:
                    local_path = os.path.join(workdir, documentation.file_name)
                    file_key = f"documentation/{documentation.file_name}"
                    lib.validate_file_type(
                        s3_client,
                        documentation.dataset.bucket,
                        workdir,
                        file_key,
                        local_path,
                        self.file_types,
                    )

                    validated_files.append(file_key)
            except Exception:
                for documentation in documentations:
                    documentation.delete()

                for file_to_delete in validated_files:
                    s3_client.delete_object(Bucket=dataset.bucket, Key=file_to_delete)
                raise

        except Exception:
            return BadRequestErrorResponse("Invalid documentation data")

        return Response(
            doc_serialized.data,
            status=201,
            headers=self.get_success_headers(doc_serialized.data),
        )
