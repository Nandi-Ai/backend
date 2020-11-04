import logging

from django.contrib.postgres.fields import JSONField
from django.db import models
from django.db.models import signals
from django.dispatch import receiver

from mainapp.models import Activity
from mainapp.utils.lib import create_limited_table_for_dataset
from mainapp.utils.monitoring import handle_event, MonitorEvents

logger = logging.getLogger(__name__)


class DatasetUser(models.Model):
    LIMITED_ACCESS = "limited_access"
    DEID_ACCESS = "deid_access"
    possible_user_permission_for_dataset = (
        (LIMITED_ACCESS, "limited_access"),
        (DEID_ACCESS, "deid_access"),
    )
    dataset = models.ForeignKey("Dataset", on_delete=models.CASCADE)
    user = models.ForeignKey("User", on_delete=models.CASCADE)
    permission = models.CharField(
        choices=possible_user_permission_for_dataset,
        max_length=32,
        null=False,
        blank=False,
    )
    permission_attributes = JSONField(null=True, default=None)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "datasets_users"
        unique_together = ("dataset", "user")

    def __str__(self):
        return (
            f"<DatasetUser dataset={self.dataset} user={self.user} "
            f"permission={self.permission} permission_attributes={self.permission_attributes}>"
        )

    @property
    def permission_key(self):
        if self.permission_attributes:
            return self.permission_attributes.get("key")

    def process(self):
        """
        process all data-sources in this datasets
        """
        if self.permission == DatasetUser.LIMITED_ACCESS:
            create_limited_table_for_dataset(self.dataset, self.permission_key)


@receiver(signals.post_save, sender=DatasetUser)
def dataset_user_post_save(sender, instance, **kwargs):
    dataset = instance.dataset
    user = instance.user
    permission = instance.permission
    Activity.objects.create(
        type="dataset permission",
        dataset=dataset,
        user=user,
        meta={
            "user_affected": str(instance.user.id),
            "action": "grant",
            "permission": instance.permission,
        },
    )

    handle_event(
        MonitorEvents.EVENT_DATASET_ADD_USER,
        {
            "dataset": dataset,
            "user": user,
            "additional_data": {
                "user_list": [user.display_name],
                "permission": permission,
            },
        },
    )


@receiver(signals.post_delete, sender=DatasetUser)
def dataset_user_post_delete(sender, instance, **kwargs):
    dataset = instance.dataset
    user = instance.user
    permission = instance.permission
    if not dataset.is_deleted:
        Activity.objects.create(
            type="dataset permission",
            dataset=dataset,
            user=user,
            meta={
                "user_affected": str(user.id),
                "action": "remove",
                "permission": "all",
            },
        )
    handle_event(
        MonitorEvents.EVENT_DATASET_REMOVE_USER,
        {
            "dataset": dataset,
            "additional_data": {
                "user_list": [user.display_name],
                "permission": permission,
            },
        },
    )
