from django.db.models import signals
from django.dispatch import receiver

from mainapp.models import Method, StudyDataset, DatasetUser
from mainapp.utils.deidentification.images_de_id import ImageDeId


@receiver(signals.pre_delete, sender=Method)
def remove_method(sender, instance, using, **kwargs):
    """
    When method is being deleted:
    * remove the dataset from all studies that have de-id permission with this method
    * remove all users with de-id permission to this dataset with the current (in deleting) method

    receiver is atomic. if any error will raised the entire transaction will be canceled.
    this is pre_delete, so if this function will fail the method will NOT be deleted.
    """
    for study_dataset in StudyDataset.objects.filter(
        permission=StudyDataset.DE_IDENTIFIED, dataset=instance.dataset
    ):
        # without str it does NOT working!
        if str(study_dataset.permission_key) == str(instance.id):
            study_dataset.delete()

    for dataset_user in DatasetUser.objects.filter(
        permission=StudyDataset.DE_IDENTIFIED, dataset=instance.dataset
    ):
        # without str it does NOT working!
        if str(dataset_user.permission_key) == str(instance.id):
            dataset_user.delete()


@receiver(signals.post_delete, sender=Method)
def remove_files(sender, instance, using, **kwargs):
    """
    Delete all de-id files related to this method
    """
    for dsrc_method in instance.data_source_methods.all():
        data_source = dsrc_method.data_source
        image_de_id = ImageDeId(
            org_name=data_source.dataset.organization.name,
            data_source=data_source,
            dsrc_method=dsrc_method,
        )
        image_de_id.delete()
