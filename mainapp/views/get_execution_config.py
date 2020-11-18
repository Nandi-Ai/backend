import logging

from rest_framework.response import Response
from rest_framework.views import APIView

from mainapp.models import Study, Execution, StudyDataset, DataSourceMethod
from mainapp.serializers import StudySerializer
from mainapp.utils.deidentification import Actions

logger = logging.getLogger(__name__)


class GetExecutionConfig(APIView):
    def __generate_deid_columns(self, data_source, dsrc_method):
        deid_columns = dict()
        for col_name in data_source.columns:
            if col_name not in dsrc_method.attributes:
                deid_columns[col_name] = data_source.columns[col_name]["glue_type"]
            elif dsrc_method.attributes[col_name]["action"] != Actions.OMIT.value:
                deid_columns[col_name] = data_source.columns[col_name]["glue_type"]
        return deid_columns

    def __generate_data_source(self, study_dataset, dsrc):
        generated_data_source = {
            "id": dsrc.id,
            "name": dsrc.name,
            "dir": dsrc.dir,
            "type": dsrc.type,
            "about": dsrc.about,
            "programmatic_name": dsrc.programmatic_name,
            "glue_table": dsrc.get_glue_table(
                study_dataset.permission, study_dataset.permission_key or str()
            ),
            "columns": {
                col_name: dsrc.columns[col_name]["glue_type"]
                for col_name in dsrc.columns
            },
        }
        if study_dataset.permission != "deid_access":
            return generated_data_source
        try:
            dsrc_method = DataSourceMethod.objects.get(
                data_source_id=dsrc.id, method_id=study_dataset.permission_key
            )
        except DataSourceMethod.DoesNotExist:
            return

        generated_data_source["columns"] = self.__generate_deid_columns(
            dsrc, dsrc_method
        )
        return generated_data_source

    # noinspection PyMethodMayBeStatic
    def get(self, request):

        execution = Execution.objects.get(execution_user=request.user)
        study = Study.objects.get(execution=execution)
        study_datasets = StudyDataset.objects.filter(study=study)

        config = {"study": StudySerializer(study).data, "datasets": list()}
        for study_dataset in study_datasets:
            data_sources = list()
            for data_source in study_dataset.dataset.data_sources.iterator():
                data_source_config = self.__generate_data_source(
                    study_dataset, data_source
                )
                if data_source_config:
                    data_sources.append(data_source_config)

            config["datasets"].append(
                {
                    "id": study_dataset.dataset.id,
                    "permission": study_dataset.permission,
                    "permission_attributes": study_dataset.permission_attributes,
                    "name": study_dataset.dataset.name,
                    "programmatic_name": study_dataset.dataset.programmatic_name,
                    "readme": study_dataset.dataset.readme,
                    "bucket": study_dataset.dataset.bucket,
                    "data_sources": data_sources,
                }
            )

        return Response(config)
