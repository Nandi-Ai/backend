from django.dispatch import receiver
from django.db.models import signals

from mainapp.models import DataSource
from mainapp.utils.aws_utils import refresh_dataset_file_share_cache


@receiver(signals.pre_save, sender=DataSource)
def refresh_cache(sender, instance, using, **kwargs):
    try:
        old_instance = DataSource.objects.get(id=instance.id)
    except DataSource.DoesNotExist:
        return

    if old_instance.state != DataSource.READY and instance.state == DataSource.READY:
        refresh_dataset_file_share_cache(org_name=instance.dataset.organization.name)
