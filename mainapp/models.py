import uuid

import logging

from django.contrib.auth.models import BaseUserManager, AbstractBaseUser
from django.contrib.auth.models import PermissionsMixin
from django.contrib.postgres.fields import JSONField
from django.db import models
from django.db.models import signals
from django.db.utils import IntegrityError
from django.dispatch import receiver

from mainapp.exceptions import BucketNotFound
from mainapp.utils import lib, aws_service

logger = logging.getLogger(__name__)


class UserManager(BaseUserManager):
    def create_user(self, email, password=None):
        """
        Creates and saves a User with the given email, date of
        birth and password.
        """
        if not email:
            raise ValueError("Users must have an email address")

        user = self.model(email=self.normalize_email(email))

        try:
            org = Organization.objects.get(default=True)
        except Organization.DoesNotExist:
            org = Organization.objects.create(name="default", default=True)

        user.organization = org

        user.set_password(password)

        user.save(using=self._db)
        return user

    def create_superuser(self, email, password):
        """
        Creates and saves a superuser with the given email, date of
        birth and password.
        """

        user = self.create_user(email, password=password)
        user.is_superuser = True
        user.is_admin = True
        user.save(using=self._db)
        return user

    def get_or_create_for_cognito(self, payload):

        cognito_id = payload["sub"]

        try:
            return self.get(cognito_id=cognito_id)
        except self.model.DoesNotExist:
            pass

        try:
            try:
                org = Organization.objects.get(default=True)
            except Organization.DoesNotExist:
                org, _ = Organization.objects.create(name="default", default=True)
            user = self.create(
                cognito_id=cognito_id,
                email=payload["email"],
                is_active=True,
                organization=org,
            )
        except IntegrityError:
            user = self.get(email=payload["email"])
            user.cognito_id = cognito_id
            user.save()

        # doesn't seem that cognito send any of those field in payload...
        if "name" in payload:
            user.name = payload["name"]
        elif "custom:name" in payload:
            user.name = payload["custom:name"]

        user.save()

        return user


class User(AbstractBaseUser, PermissionsMixin):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    email = models.EmailField(verbose_name="email address", max_length=255, unique=True)

    is_active = models.BooleanField(default=True)
    is_superuser = models.BooleanField(default=False)
    is_admin = models.BooleanField(default=False)
    name = models.CharField(max_length=32, blank=True, null=True)
    first_login = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    organization = models.ForeignKey(
        "Organization", on_delete=models.CASCADE, related_name="users", null=True
    )
    cognito_id = models.CharField(max_length=255, blank=True, null=True)
    is_execution = models.BooleanField(default=False)

    @property
    def data_sources(self):
        data_sources = DataSource.objects.none()
        for dataset in self.datasets.all():
            data_sources = data_sources | dataset.data_sources.all()

        return data_sources

    @property
    def related_studies(self):
        studies_ids = []
        studies_ids = studies_ids + [s.id for s in self.studies.all()]
        for dataset in self.admin_datasets.all():
            studies_ids = studies_ids + [s.id for s in dataset.studies.all()]
        studies = Study.objects.filter(
            id__in=studies_ids
        )  # no need set. return one item even if id appears multiple times.
        return studies

    @property
    def datasets(self):
        discoverable_datasets = (
            Dataset.objects.exclude(is_discoverable=False)
            | self.full_access_datasets.filter(is_discoverable=False)
            | self.aggregated_datasets.filter(is_discoverable=False)
            | self.admin_datasets.filter(is_discoverable=False)
        ).distinct()
        not_archived_datasets = (
            Dataset.objects.exclude(state="archived")
            | self.admin_datasets.filter(state="archived")
        ).distinct()

        return discoverable_datasets & not_archived_datasets

    @property
    def requests_for_me(self):
        requests = Request.objects.none()
        for dataset in self.admin_datasets.all():
            requests = requests | dataset.requests.all()

        return requests

    @property
    def my_requests(self):
        requests = Request.objects.filter(user_requested=self)

        return requests

    objects = UserManager()

    USERNAME_FIELD = "email"

    # REQUIRED_FIELDS = []

    def __str__(self):
        return self.email

    def has_perm(self, perm, obj=None):
        """Does the user have a specific permission?"""
        # Simplest possible answer: Yes, always
        return True

    def has_module_perms(self, app_label):
        """Does the user have permissions to view the app `app_label`?"""
        # Simplest possible answer: Yes, always
        return True

    @property
    def is_staff(self):
        """Is the user a member of staff?"""
        # Simplest possible answer: All admins are staff
        return self.is_admin

    class Meta:
        db_table = "users"

    def permission(self, dataset):
        if self in dataset.admin_users.all():
            return "admin"
        if self in dataset.full_access_users.all():
            return "full_access"
        if self in dataset.aggregated_users.all():
            return "aggregated_access"
        # this function can also return None.....


class Organization(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    logo = models.CharField(max_length=255, null=True)
    default = models.BooleanField(default=False)

    def set_default(self):
        Organization.objects.all().update(default=False)
        self.default = True
        self.save()

    class Meta:
        db_table = "organizations"


class Study(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True, max_length=255)
    datasets = models.ManyToManyField(
        "Dataset", related_name="studies", through="StudyDataset"
    )
    users = models.ManyToManyField("User", related_name="studies")
    user_created = models.ForeignKey(
        "User", on_delete=models.SET_NULL, related_name="studies_created", null=True
    )
    execution = models.ForeignKey(
        "Execution", on_delete=models.CASCADE, related_name="studies", null=True
    )
    tags = models.ManyToManyField("Tag", related_name="study_tags")
    updated_at = models.DateTimeField(auto_now=True)
    cover = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        db_table = "studies"

    @property
    def bucket(self):
        return "lynx-workspace-" + str(self.id)

    def delete_bucket(self, org_name):
        logger.info(f"Deleting bucket {self.bucket} for study {self.id}")
        lib.delete_bucket(bucket_name=self.bucket, org_name=org_name)

    def __str__(self):
        return f"<Study id={self.id} name={self.name}>"


class StudyDataset(models.Model):
    FULL_ACCESS = "full_access"
    AGGREGATED_ACCESS = "aggregated_access"
    possible_dataset_permission_for_study = (
        (FULL_ACCESS, "full_access"),
        (AGGREGATED_ACCESS, "aggregated_access"),
    )
    dataset = models.ForeignKey("Dataset", on_delete=models.CASCADE)
    study = models.ForeignKey("Study", on_delete=models.CASCADE)
    permission = models.CharField(
        choices=possible_dataset_permission_for_study,
        max_length=32,
        null=False,
        blank=False,
    )
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "studies_datasets"
        unique_together = ("dataset", "study")

    def __str__(self):
        return f"<StudyDataset dataset={self.dataset} study={self.study} permission={self.permission}>"


@receiver(signals.pre_delete, sender=Study)
def delete_study(sender, instance, **kwargs):
    study = instance
    try:
        study.delete_bucket(org_name=instance.user_created.organization.name)
    except BucketNotFound as e:
        logger.warning(
            f"Bucket {e.bucket_name} was not found for study id {study.id} at delete bucket operation"
        )


class Dataset(models.Model):
    states = (("public", "public"), ("private", "private"), ("archived", "archived"))

    possible_default_user_permissions_for_private_dataset = (
        ("none", "none"),
        ("aggregated_access", "aggregated_access"),
    )
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True, max_length=255)
    readme = models.TextField(blank=True, null=True)
    admin_users = models.ManyToManyField("User", related_name="admin_datasets")
    aggregated_users = models.ManyToManyField(
        "User", related_name="aggregated_datasets"
    )
    full_access_users = models.ManyToManyField(
        "User", related_name="full_access_datasets"
    )
    user_created = models.ForeignKey(
        "User", on_delete=models.SET_NULL, related_name="datasets_created", null=True
    )
    tags = models.ManyToManyField("Tag", related_name="dataset_tags")
    state = models.CharField(choices=states, max_length=32)
    is_discoverable = models.BooleanField(blank=False, null=False)
    default_user_permission = models.CharField(
        choices=possible_default_user_permissions_for_private_dataset,
        max_length=32,
        null=True,
    )
    bucket_override = models.CharField(max_length=255, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)
    glue_database_override = models.CharField(max_length=255, blank=True, null=True)
    programmatic_name = models.CharField(max_length=255, blank=True, null=True)
    organization = models.ForeignKey(
        "Organization", on_delete=models.DO_NOTHING, related_name="datasets", null=True
    )
    cover = models.CharField(max_length=255, blank=True, null=True)
    ancestor = models.ForeignKey(
        "self", on_delete=models.SET_NULL, related_name="children", null=True
    )

    class Meta:
        db_table = "datasets"

    @property
    def permitted_users(self):
        return (
            self.aggregated_users | self.admin_users | self.full_access_users
        ).distinct()

    @property
    def glue_database(self):
        if self.glue_database_override:
            return self.glue_database_override
        return "dataset-" + str(self.id)

    @property
    def bucket(self):
        if self.bucket_override:
            return self.bucket_override
        return "lynx-dataset-" + str(self.id)

    def delete_bucket(self, org_name):
        logger.info(f"Deleting bucket {self.bucket} for dataset {self.id}")
        lib.delete_bucket(bucket_name=self.bucket, org_name=org_name)

    def __str__(self):
        return f"<Dataset id={self.id} name={self.name}>"


@receiver(signals.pre_delete, sender=Dataset)
def delete_dataset(sender, instance, **kwargs):
    dataset = instance
    try:
        dataset.delete_bucket(org_name=instance.user_created.organization.name)
    except BucketNotFound as e:
        logger.warning(
            f"Bucket {e.bucket_name} was not found for dataset id {dataset.id} at delete bucket operation"
        )


class DataSource(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    dir = models.CharField(null=True, blank=True, max_length=255)
    s3_objects = JSONField(null=True, blank=True, default=None)
    dataset = models.ForeignKey(
        "Dataset", on_delete=models.CASCADE, related_name="data_sources"
    )
    type = models.CharField(null=True, blank=True, max_length=32)
    about = models.TextField(null=True, blank=True, max_length=2048)
    state = models.CharField(null=True, blank=True, max_length=32)
    programmatic_name = models.CharField(max_length=255, blank=True, null=True)
    ancestor = models.ForeignKey(
        "self", on_delete=models.SET_NULL, related_name="children", null=True
    )
    cohort = JSONField(null=True, blank=True, default=None)

    class Meta:
        db_table = "data_sources"
        unique_together = (("name", "dataset"),)

    @property
    def glue_table(self):
        if not self.type == "structured":
            return None
        name = self.dir.translate(
            {ord(c): "_" for c in "!@#$%^&*()[]{};:,./<>?\|`~-=_+\ "}
        )
        name = name.lower()
        return name

    @property
    def bucket(self):
        return self.dataset.bucket


@receiver(signals.pre_delete, sender=DataSource)
def delete_data_source(sender, instance, **kwargs):
    data_source = instance
    org_name = instance.dataset.organization.name
    logger.info(
        f"Deleting data source {data_source.name} for following dataset.id {data_source.id}"
    )
    if data_source.glue_table:
        glue_client = aws_service.create_glue_client(org_name=org_name)
        try:
            glue_client.delete_table(
                DatabaseName=data_source.dataset.glue_database,
                Name=data_source.glue_table,
            )
            logger.info(f"Removed glue table: {data_source.glue_table} successfully")
        except glue_client.exceptions.EntityNotFoundException as e:
            logger.warning("Unexpected error when deleting glue table", error=e)

    if data_source.dir:
        if data_source.dir == "":
            logger.warning(
                f"Warning: data source {data_source.name} {data_source.id} 'dir' field is an empty string ('')"
            )
        else:  # delete dir in bucket
            s3_resource = aws_service.create_s3_resource(org_name=org_name)
            try:
                bucket = s3_resource.Bucket(data_source.bucket)
                bucket.objects.filter(Prefix=data_source.dir + "/").delete()
            except s3_resource.exceptions.NoSuchKey:
                logger.warning(
                    f"Warning no such key {data_source.dir} in {bucket}. "
                    f"Ignoring deleting dir while deleting data_source {data_source.name} ({data_source.id})"
                )
            except s3_resource.exceptions.NoSuchBucket:
                logger.warning(
                    f"Warning no such bucket {bucket} while trying to delete dir {dir}"
                )

    logger.info(
        f"Data source {data_source.name} ({data_source.id}) was deleted successfully"
    )


class Tag(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    category = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        db_table = "tags"
        unique_together = (("name", "category"),)

    def __str__(self):
        return f"<Category name={self.name} category={self.category}>"


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


class Activity(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    ts = models.DateTimeField(auto_now_add=True)
    user = models.ForeignKey(
        "User", on_delete=models.SET_NULL, related_name="activities", null=True
    )
    dataset = models.ForeignKey(
        "Dataset", on_delete=models.SET_NULL, related_name="activities", null=True
    )
    study = models.ForeignKey(
        "Study", on_delete=models.SET_NULL, related_name="activities", null=True
    )
    type = models.CharField(null=True, blank=True, max_length=32)
    note = models.CharField(null=True, blank=True, max_length=2048)
    meta = JSONField(null=True, blank=True, default=None)

    class Meta:
        db_table = "activities"


class Request(models.Model):
    types = (("dataset_access", "dataset_access"),)

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    user_requested = models.ForeignKey(
        "User", on_delete=models.CASCADE, related_name="requests", null=True
    )
    dataset = models.ForeignKey(
        "Dataset", on_delete=models.CASCADE, related_name="requests", null=True
    )
    study = models.ForeignKey(
        "Study", on_delete=models.CASCADE, related_name="requests", null=True
    )
    type = models.CharField(choices=types, max_length=32)
    note = models.CharField(null=True, blank=True, max_length=2048)
    permission = models.CharField(null=True, blank=True, max_length=32)
    state = models.CharField(null=True, blank=True, default="pending", max_length=32)

    class Meta:
        db_table = "requests"


class Documentation(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    dataset = models.ForeignKey(
        "Dataset",
        on_delete=models.CASCADE,
        related_name="documentation",
        max_length=255,
    )
    file_name = models.CharField(max_length=255, default=None)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "documentations"
