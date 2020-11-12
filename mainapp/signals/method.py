from django.db.models import signals
from django.dispatch import receiver

from mainapp.models import Method, StudyDataset, DatasetUser
from mainapp.utils.aws_utils import refresh_dataset_file_share_cache


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
def refresh_dataset_storage(sender, instance, using, **kwargs):
    """
    Refreshes the File share of the dataset related to the method
    """

    refresh_dataset_file_share_cache(org_name=instance.dataset.organization.name)
