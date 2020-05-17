import logging

from rest_framework.response import Response
from rest_framework.views import APIView

from mainapp.models import Study, Execution, StudyDataset
from mainapp.serializers import DataSourceSerializer, DatasetSerializer, StudySerializer

logger = logging.getLogger(__name__)


class GetExecutionConfig(APIView):
    # noinspection PyMethodMayBeStatic
    def get(self, request):

        execution = Execution.objects.get(execution_user=request.user)
        study = Study.objects.get(execution=execution)
        study_datasets = StudyDataset.objects.filter(study=study)

        config = {"study": StudySerializer(study).data, "datasets": []}
        for study_dataset in study_datasets:
            dataset_ser = DatasetSerializer(study_dataset.dataset).data
            dataset_ser["permission"] = study_dataset.permission
            dataset_ser["data_sources"] = []
            for data_source in study_dataset.dataset.data_sources.all():
                data_source_ser = DataSourceSerializer(data_source).data
                dataset_ser["data_sources"].append(data_source_ser)

            config["datasets"].append(dataset_ser)

        return Response(config)
