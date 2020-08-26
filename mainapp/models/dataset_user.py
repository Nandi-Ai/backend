import logging

from django.contrib.postgres.fields import JSONField
from django.db import models
from django.db.models import signals
from django.dispatch import receiver

from mainapp.models import Activity
from mainapp.utils.elasticsearch_service import MonitorEvents, ElasticsearchService

logger = logging.getLogger(__name__)


class DatasetUser(models.Model):
    LIMITED_ACCESS = "limited_access"
    possible_user_permission_for_dataset = ((LIMITED_ACCESS, "limited_access"),)
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

    def get_limited_value(self):
        return self.permission_attributes.get("key")


def monitor_dataset_user_event(dataset, event_type, user, additional_data=None):
    ElasticsearchService.write_monitoring_event(
        event_type=event_type,
        user_ip=None,
        dataset_id=dataset.id,
        dataset_name=dataset.name,
        user_name=user.display_name,
        datasource_id="",
        datasource_name="",
        environment_name=dataset.organization.name,
        user_organization=user.organization.name,
        additional_data=additional_data if additional_data else None,
    )

    logger.info(
        f"Dataset Event: {event_type.value} "
        f"on dataset {dataset.name}:{dataset.id} "
        f"by user {user.display_name}. "
        f"additional data for event : {str(additional_data)}"
    )


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

    monitor_dataset_user_event(
        event_type=MonitorEvents.EVENT_DATASET_ADD_USER,
        dataset=dataset,
        user=user,
        additional_data={"user_list": [user.display_name], "permission": permission},
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

    monitor_dataset_user_event(
        event_type=MonitorEvents.EVENT_DATASET_REMOVE_USER,
        dataset=dataset,
        user=user,
        additional_data={"user_list": [user.display_name], "permission": permission},
    )
