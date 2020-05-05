import uuid

from django.db import models


class Execution(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    real_user = models.ForeignKey("User", on_delete=models.CASCADE, null=True)
    execution_user = models.ForeignKey(
        "User", on_delete=models.CASCADE, related_name="the_execution", null=True
    )

    class Meta:
        db_table = "executions"

    @property
    def token(self):
        return str(self.id).split("-")[-1]
