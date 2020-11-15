import logging
import uuid
from mainapp import settings

from django.contrib.auth.base_user import BaseUserManager
from django.contrib.auth.models import AbstractBaseUser
from django.contrib.auth.models import PermissionsMixin
from django.db import models
from django.db.utils import IntegrityError

from mainapp.models import (
    Organization,
    DataSource,
    Study,
    Dataset,
    Request,
    DatasetUser,
)

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

    def determine_user_organization(self, email):
        org = None
        for (
            identity_provider,
            corresponding_org,
        ) in settings.COGNITO_SAML_PROVIDERS.items():
            if identity_provider in email:
                org = Organization.objects.get(name=corresponding_org)
                break
        if not org:
            try:
                org = Organization.objects.get(default=True)
            except Organization.DoesNotExist:
                org, _ = Organization.objects.create(name="default", default=True)
        return org

    def get_or_create_for_cognito(self, payload):

        cognito_id = payload["sub"]

        try:
            current_user = self.get(cognito_id=cognito_id)
            if not current_user.phone_number:
                current_user.phone_number = payload["phone_number"]
                current_user.save()
            return current_user
        except self.model.DoesNotExist:
            pass

        try:
            org = self.determine_user_organization(payload["email"])
            user = self.create(
                cognito_id=cognito_id,
                email=payload["email"],
                phone_number=payload["phone_number"],
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
    phone_number = models.CharField(max_length=15, blank=True, null=True)
    title = models.CharField(max_length=128, blank=True, null=True)
    department = models.CharField(max_length=128, blank=True, null=True)
    linkedin = models.CharField(max_length=60, blank=True, null=True)
    bio = models.CharField(max_length=512, blank=True, null=True)
    interests = models.CharField(max_length=512, blank=True, null=True)
    photo = models.CharField(max_length=255, blank=True, null=True)
    tags = models.ManyToManyField("Tag", related_name="users", blank=True)
    agreed_eula_file_path = models.CharField(max_length=255, blank=True, null=True)

    @property
    def latest_eula_file_path(self):
        return settings.EULA_FILE_PATH

    @property
    def is_signed_eula(self):
        return settings.EULA_FILE_PATH == self.agreed_eula_file_path

    @property
    def data_sources(self):
        data_sources = DataSource.objects.none()
        for dataset in self.datasets.all():
            data_sources = data_sources | dataset.data_sources.all()

        return data_sources

    @property
    def related_studies(self):
        studies_ids = []
        # get all studies which the collaborator in
        studies_ids = studies_ids + [s.id for s in self.studies.all()]
        # for activity purposes, we need to return also the studies which uses dataset which the user admins.
        for dataset in self.admin_datasets.all():
            studies_ids = studies_ids + [s.id for s in dataset.studies.all()]
        studies = Study.objects.filter(
            id__in=studies_ids
        )  # no need set. return one item even if id appears multiple times.
        return studies

    @property
    def datasets(self):
        if self.is_execution:
            execution = self.the_execution.last()
            study = Study.objects.filter(execution=execution).last()
            return study.datasets.filter(is_deleted=False).all()

        else:
            discoverable_datasets = (
                Dataset.objects.exclude(is_discoverable=False)
                | self.full_access_datasets.filter(is_discoverable=False)
                | self.aggregated_datasets.filter(is_discoverable=False)
                | self.admin_datasets.filter(is_discoverable=False)
                | self.permitted_datasets.filter(is_discoverable=False)
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

    @property
    def display_name(self):
        return self.name or self.email

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
        try:
            return DatasetUser.objects.get(
                user_id=self.id, dataset_id=dataset.id
            ).permission
        except DatasetUser.DoesNotExist:
            pass
        return dataset.default_user_permission
        # this function can also return None.....

    def permission_attributes(self, dataset):
        try:
            return DatasetUser.objects.get(
                user_id=self.id, dataset_id=dataset.id
            ).permission_attributes.get("key")
        except DatasetUser.DoesNotExist:
            if dataset.permission_attributes:
                return dataset.permission_attributes.get("key")
