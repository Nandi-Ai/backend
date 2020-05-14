import json
import logging
import os
import shutil
import threading
import time
import uuid

import boto3
import botocore.exceptions
import dateparser
import pyreadstat
from botocore.config import Config
from django.core import exceptions
from django.db import transaction
from django.db.utils import IntegrityError
from rest_framework.decorators import action
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.viewsets import ModelViewSet, ReadOnlyModelViewSet
from rest_framework_swagger.views import get_swagger_view

# noinspection PyPackageRequirements
from slugify import slugify

from mainapp import resources, settings
from mainapp.exceptions import (
    UnableToGetGlueColumns,
    UnsupportedColumnTypeError,
    QueryExecutionError,
    InvalidExecutionId,
    MaxExecutionReactedError,
    BucketNotFound,
)
from mainapp.models import (
    User,
    Organization,
    Study,
    Dataset,
    DataSource,
    Tag,
    Execution,
    Activity,
    Request,
    Documentation,
    StudyDataset,
)
from mainapp.serializers import (
    UserSerializer,
    OrganizationSerializer,
    DocumentationSerializer,
    TagSerializer,
    DataSourceSerializer,
    ActivitySerializer,
    RequestSerializer,
    DatasetSerializer,
    StudySerializer,
    SimpleQuerySerializer,
    QuerySerializer,
    CohortSerializer,
)
from mainapp.utils import devexpress_filtering
from mainapp.utils import statistics, lib, aws_service
from mainapp.utils.response_handler import (
    ErrorResponse,
    ForbiddenErrorResponse,
    NotFoundErrorResponse,
    ConflictErrorResponse,
    BadRequestErrorResponse,
    UnimplementedErrorResponse,
)


logger = logging.getLogger(__name__)


class TagViewSet(ReadOnlyModelViewSet):
    serializer_class = TagSerializer
    queryset = Tag.objects.all()
