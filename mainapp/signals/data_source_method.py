from django.dispatch import receiver
from django.db.models import signals

from mainapp.models import DataSourceMethod
from mainapp.utils.deidentification.images_de_id import ImageDeId


@receiver(signals.post_delete, sender=DataSourceMethod)
def remove_files(sender, instance, using, **kwargs):
    """
    Delete all de-id files related to this data source method
    """
    data_source = instance.data_source
    image_de_id = ImageDeId(
        org_name=data_source.dataset.organization.name,
        data_source=data_source,
        dsrc_method=instance,
    )
    image_de_id.delete()
