from django.contrib.auth.models import (BaseUserManager, AbstractBaseUser)
from django.contrib.auth.models import PermissionsMixin
from django.db import models
from django.db.utils import IntegrityError
from django.contrib.postgres.fields import JSONField

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

        try:
            u = self.get(email=payload['email'])
            if not u.cognito_id or u.cognito_id != payload['sub']:
                u.cognito_id = payload['sub']
                u.save()

            return u

        except self.model.DoesNotExist:

            u = self.create(cognito_id=payload['sub'], email=payload['email'], is_active=True)

            if 'organization' in payload:
                organization_name = payload['organization']
                organization, _ = Organization.objects.get_or_create(name = organization_name)

                u.organization = organization
                u.save()

            return u


class User(AbstractBaseUser, PermissionsMixin):
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
    organization = models.ForeignKey('Organization', on_delete=models.DO_NOTHING, related_name="users", null=True)
    cognito_id = models.CharField(max_length=255, blank=True, null=True)
    is_execution = models.BooleanField(default=False)

    @property
    def data_sources(self):
        data_sources = DataSource.objects.none()
        for dataset in self.datasets.all():
            data_sources = data_sources | dataset.data_sources.all()
            return data_sources

    @property
    def datasets(self):
        #all public datasets, datasets that the user have aggregated access accept archived, datasets that the user has admin access, datasets that the user have full access permission accept archived.
        datasets = (Dataset.objects.filter(state__in=["public", "private"]) | self.admin_datasets.filter(state = "archived").distinct())
        return datasets

    objects = UserManager()

    USERNAME_FIELD = 'email'
    #REQUIRED_FIELDS = []

    def __str__(self):
        return self.email

    def has_perm(self, perm, obj=None):
        "Does the user have a specific permission?"
        # Simplest possible answer: Yes, always
        return True

    def has_module_perms(self, app_label):
        "Does the user have permissions to view the app `app_label`?"
        # Simplest possible answer: Yes, always
        return True

    @property
    def is_staff(self):
        "Is the user a member of staff?"
        # Simplest possible answer: All admins are staff
        return self.is_admin

    class Meta:
        # need to manualy edit the primary key and change it from timestamp to [patient_id,timestamp] in that order
        db_table = 'users'

class Organization(models.Model):
    name = models.CharField(max_length=255, primary_key = True)

    class Meta:
        db_table = 'organizations'


class Study(models.Model):
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True, max_length=255)
    # organization = models.ForeignKey("Organization", on_delete=models.DO_NOTHING, related_name="studies")
    datasets = models.ManyToManyField('Dataset', related_name="studies")
    users = models.ManyToManyField('User', related_name="studies")
    user_created = models.ForeignKey('User', on_delete=models.DO_NOTHING, related_name="studies_created", null=True)
    execution = models.ForeignKey("Execution", on_delete=models.DO_NOTHING, related_name="studies", null=True)
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
        ("aggregated", "aggregated"),
    )

    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True, max_length=255)
    readme = models.TextField(blank=True, null=True)
    admin_users = models.ManyToManyField('User', related_name="admin_datasets")
    aggregated_users = models.ManyToManyField('User', related_name="aggregated_datasets")
    full_access_users = models.ManyToManyField('User', related_name="full_access_datasets")
    user_created = models.ForeignKey('User', on_delete=models.DO_NOTHING, related_name="datasets_created", null=True)
    users_requested_full_access = models.ManyToManyField('User', related_name="requested_full_access_for_datasets")
    users_requested_aggregated_access = models.ManyToManyField('User', related_name="requested_aggregated_access_for_datasets")
    tags = models.ManyToManyField('Tag', related_name="dataset_tags")
    state = models.CharField(choices=states, max_length=32)
    default_user_permission = models.CharField(choices=possible_default_user_permissions_for_private_dataset, max_length=32, null=True)
    bucket = models.CharField(max_length=255, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now = True)
    glue_database = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        db_table = 'datasets'


class DataSource(models.Model):
    name = models.CharField(max_length=255)
    dir = models.CharField(null=True, blank=True, max_length=255)
    s3_objects = JSONField(null = True, blank = True, default = None)
    dataset = models.ForeignKey('Dataset', on_delete=models.DO_NOTHING, related_name="data_sources")
    type = models.CharField(null=True, blank=True, max_length=32)
    about = models.TextField(null=True, blank=True, max_length=2048)
    columns = JSONField(null = True, blank = True, default = None)
    preview = JSONField(null = True, blank = True, default = None)
    state = models.CharField(null=True, blank=True, max_length=32)

    class Meta:
        db_table = 'data_sources'
        unique_together = (("name", "dataset"),)


class Tag(models.Model):
    name = models.CharField(max_length=32)
    category = models.CharField(max_length=32, null=True, blank=True)

    class Meta:
        db_table = 'tags'


class Execution(models.Model):
    identifier = models.CharField(max_length=255, null=True)
    user = models.ForeignKey('User', on_delete=models.DO_NOTHING, related_name="executions", null=True)
    # study = models.ForeignKey('Study', on_delete=models.DO_NOTHING, related_name="executions", null=True)

    class Meta:
        db_table = 'executions'
