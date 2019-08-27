from django.contrib.auth.models import (BaseUserManager, AbstractBaseUser)
from django.contrib.auth.models import PermissionsMixin
from django.db import models
from django.db.utils import IntegrityError
from django.contrib.postgres.fields import JSONField
import uuid


class UserManager(BaseUserManager):
    def create_user(self, email, password=None):
        """
        Creates and saves a User with the given email, date of
        birth and password.
        """
        if not email:
            raise ValueError('Users must have an email address')

        user = self.model(
            email=self.normalize_email(email),
        )

        user.set_password(password)
        user.save(using=self._db)
        return user

    def create_superuser(self, email, password):
        """
        Creates and saves a superuser with the given email, date of
        birth and password.
        """

        user = self.create_user(
            email,
            password=password,
        )
        user.is_superuser = True
        user.is_admin = True
        user.save(using=self._db)
        return user

    def get_or_create_for_cognito(self, payload):

        # print(payload)

        cognito_id = payload['sub']

        try:
            return self.get(cognito_id=cognito_id)
        except self.model.DoesNotExist:
            pass

        try:
            user = self.create(
                cognito_id=cognito_id,
                email=payload['email'],
                is_active=True)
        except IntegrityError:
            user = self.get(email=payload['email'])
            user.cognito_id=cognito_id
            user.save()

        return user


class User(AbstractBaseUser, PermissionsMixin):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    email = models.EmailField(
        verbose_name='email address',
        max_length=255,
        unique=True,
    )

    is_active = models.BooleanField(default=True)
    is_superuser = models.BooleanField(default=False)
    is_admin = models.BooleanField(default=False)
    name = models.CharField(max_length=32, blank=True, null=True)
    first_login = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    organization = models.ForeignKey('Organization', on_delete=models.CASCADE, related_name="users", null=True)
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
        print("here")
        studies_ids = []
        studies_ids = studies_ids + [s.id for s in self.studies_created.all()]
        for dataset in self.admin_datasets.all():
            studies_ids = studies_ids + [s.id for s in dataset.studies.all()]
        studies = Study.objects.filter(
            id__in=studies_ids)  # no need set. return one item even if id appears multiple times.
        return studies

    @property
    def datasets(self):
        # all public datasets, datasets that the user have aggregated access accept archived,
        # datasets that the user has admin access, datasets that the user have full access permission accept archived.

        datasets = (Dataset.objects.exclude(state="archived") | self.admin_datasets.filter(
            # this method seems to return duplicate items because something related to the admin_datasets(many to many)
            state="archived")).distinct()
        # datasets = Dataset.objects.exclude(state="archived").union(self.admin_datasets.filter(state = "archived"))
        return datasets

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

    USERNAME_FIELD = 'email'

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
        # need to manually edit the primary key and change it from timestamp to [patient_id,timestamp] in that order
        db_table = 'users'

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

    class Meta:
        db_table = 'organizations'


class Study(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True, max_length=255)
    # organization = models.ForeignKey("Organization", on_delete=models.DO_NOTHING, related_name="studies")
    datasets = models.ManyToManyField('Dataset', related_name="studies")
    users = models.ManyToManyField('User', related_name="studies")
    user_created = models.ForeignKey('User', on_delete=models.SET_NULL, related_name="studies_created", null=True)
    execution = models.ForeignKey("Execution", on_delete=models.CASCADE, related_name="studies", null=True)
    tags = models.ManyToManyField('Tag', related_name="study_tags")
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'studies'
        # unique_together = (("name", "organization"),)


class Dataset(models.Model):
    states = (
        ("public", "public"),
        ("private", "private"),
        ("archived", "archived")
    )

    possible_default_user_permissions_for_private_dataset = (
        ("none", "none"),
        ("aggregated_access", "aggregated_access"),
    )
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True, max_length=255)
    readme = models.TextField(blank=True, null=True)
    admin_users = models.ManyToManyField('User', related_name="admin_datasets")
    aggregated_users = models.ManyToManyField('User', related_name="aggregated_datasets")
    full_access_users = models.ManyToManyField('User', related_name="full_access_datasets")
    user_created = models.ForeignKey('User', on_delete=models.SET_NULL, related_name="datasets_created", null=True)
    tags = models.ManyToManyField('Tag', related_name="dataset_tags")
    state = models.CharField(choices=states, max_length=32)
    default_user_permission = models.CharField(choices=possible_default_user_permissions_for_private_dataset,
                                               max_length=32, null=True)
    bucket = models.CharField(max_length=255, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True)
    glue_database = models.CharField(max_length=255, blank=True, null=True)
    programmatic_name = models.CharField(max_length=255, blank=True, null=True)
    organization = models.ForeignKey('Organization', on_delete=models.DO_NOTHING, related_name="datasets", null=True)

    class Meta:
        db_table = 'datasets'

    @property
    def permitted_users(self):
        return (self.aggregated_users | self.admin_users | self.full_access_users).distinct()


class DataSource(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    dir = models.CharField(null=True, blank=True, max_length=255)
    s3_objects = JSONField(null=True, blank=True, default=None)
    dataset = models.ForeignKey('Dataset', on_delete=models.CASCADE, related_name="data_sources")
    type = models.CharField(null=True, blank=True, max_length=32)
    about = models.TextField(null=True, blank=True, max_length=2048)
    columns = JSONField(null=True, blank=True, default=None)
    preview = JSONField(null=True, blank=True, default=None)
    state = models.CharField(null=True, blank=True, max_length=32)
    programmatic_name = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        db_table = 'data_sources'
        unique_together = (("name", "dataset"),)

    @property
    def glue_table(self):
        if self.type != "structured":
            return

        return self.dir.replace("-", "_")


class Tag(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    category = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        db_table = 'tags'


class Execution(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    # identifier = models.CharField(max_length=255, null=True)
    real_user = models.ForeignKey('User', on_delete=models.CASCADE, null=True)
    execution_user = models.ForeignKey('User', on_delete=models.CASCADE, related_name="the_execution", null=True)

    # study = models.ForeignKey('Study', on_delete=models.DO_NOTHING, related_name="executions", null=True)

    class Meta:
        db_table = 'executions'

    @property
    def token(self):
        return str(self.id).split("-")[-1]


class Activity(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    ts = models.DateTimeField(auto_now_add=True)
    user = models.ForeignKey('User', on_delete=models.SET_NULL, related_name="activities", null=True)
    dataset = models.ForeignKey('Dataset', on_delete=models.SET_NULL, related_name="activities", null=True)
    study = models.ForeignKey('Study', on_delete=models.SET_NULL, related_name="activities", null=True)
    type = models.CharField(null=True, blank=True, max_length=32)
    # action = models.CharField(null=True, blank=True, max_length=1024)
    note = models.CharField(null=True, blank=True, max_length=2048)
    meta = JSONField(null=True, blank=True, default=None)

    class Meta:
        db_table = 'activities'
        # in future it is possible to optimize this table by creating primary_key=(ts,user)
        # while removing the id and the unique_together constraint.
        # (for now django not supports combined primary key but it can be achieved manually
        # with adding to the migration:
        # migrations.RunSQL(
        # "ALTER TABLE entries DROP CONSTRAINT entries_pkey; ALTER TABLE entries ADD PRIMARY KEY (user_id ,ts)


class Request(models.Model):
    types = (
        ("dataset_access", "dataset_access"),
    )

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    user_requested = models.ForeignKey('User', on_delete=models.CASCADE, related_name="requests", null=True)
    dataset = models.ForeignKey('Dataset', on_delete=models.CASCADE, related_name="requests", null=True)
    study = models.ForeignKey('Study', on_delete=models.CASCADE, related_name="requests", null=True)
    type = models.CharField(choices=types, max_length=32)
    note = models.CharField(null=True, blank=True, max_length=2048)
    permission = models.CharField(null=True, blank=True, max_length=32)
    state = models.CharField(null=True, blank=True, default="pending", max_length=32)

    class Meta:
        db_table = 'requests'

        # in future it is possible to optimize this table by creating primary_key=(ts,user)
        # while removing the id and the unique_together constraint.
        # (for now django not supports combined primary key but it can be achieved manually
        # with adding to the migration: # migrations.RunSQL(
        # "ALTER TABLE entries DROP CONSTRAINT entries_pkey; ALTER TABLE entries ADD PRIMARY KEY (user_id ,ts)
