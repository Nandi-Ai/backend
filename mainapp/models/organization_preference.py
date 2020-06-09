import uuid

from django.db import models


class OrganizationPreference(models.Model):
    CAN_COPY_PASTE_IN_NOTEBOOK = "can_copy_paste_in_notebook"
    possible_keys = ((CAN_COPY_PASTE_IN_NOTEBOOK, "can_copy_paste_in_notebook"),)

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    organization = models.ForeignKey(
        "Organization",
        on_delete=models.DO_NOTHING,
        related_name="preferences",
        null=False,
        blank=False,
    )
    key = models.CharField(
        choices=possible_keys, max_length=32, null=False, blank=False
    )
    value = models.CharField(max_length=255, null=False, blank=False)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "organizations_preferences"
        unique_together = ("organization", "key")
