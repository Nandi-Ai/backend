from mainapp.utils.deidentification import (
    GLUE_LYNX_TYPE_MAPPING,
    GLUE_DATA_TYPE_MAPPING,
    LynxDataTypeNames,
    DataTypes,
)
from mainapp.utils.lib import get_columns_types


def generate_columns(data_source):
    column_types = get_columns_types(
        org_name=data_source.dataset.organization.name,
        glue_database=f"dataset-{data_source.dataset.id}",
        glue_table=data_source.glue_table,
    )
    data_source.columns = {
        col["Name"]: {
            "glue_type": col["Type"],
            "data_type": GLUE_DATA_TYPE_MAPPING.get(
                col["Type"], DataTypes.STRING.value
            ),
            "lynx_type": GLUE_LYNX_TYPE_MAPPING.get(
                col["Type"], LynxDataTypeNames.TEXT.value
            ),
            "display_name": col["Name"],
        }
        for col in column_types
    }
    data_source.save()
