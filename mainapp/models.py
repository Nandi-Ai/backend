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

        print(payload)
        cognito_id = payload['sub']

        #user exists:
        try:
            user = self.get(cognito_id=cognito_id)
            #update organization from cognito if changed:
            return user

        except self.model.DoesNotExist:
            pass


        #its a new user:

        try:
            user = self.create(
                cognito_id=cognito_id,
                email=payload['email'],
                is_active=True
            )

        except IntegrityError:
            raise

        if 'organization' in payload:
            organization_name = payload['organization']
            organization, _ = Organization.objects.get_or_create(name = organization_name)

            user.organization = organization
            user.save()

        return user

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
    is_execution = models.BooleanField(default = False)


    objects = UserManager()

    USERNAME_FIELD = 'email'
    #REQUIRED_FIELDS = []

    def __str__(self):              # __unicode__ on Python 2
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
    # organization = models.ForeignKey("Organization", on_delete=models.DO_NOTHING, related_name="studies")
    datasets = models.ManyToManyField('Dataset', related_name="studies")
    users = models.ManyToManyField('User', related_name="studies")
    user_created = models.ForeignKey('User', on_delete=models.DO_NOTHING, related_name="studies_created", null=True)
    execution = models.ForeignKey("Execution", on_delete=models.DO_NOTHING, related_name="studies", null=True)

    class Meta:
        db_table = 'studies'
        # unique_together = (("name", "organization"),)

class Dataset(models.Model):
    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True, max_length=255)
    readme = models.TextField(blank=True, null=True)
    users = models.ManyToManyField('User', related_name="datasets")
    user_created = models.ForeignKey('User', on_delete=models.DO_NOTHING, related_name="datasets_created", null=True)
    tags = models.ManyToManyField('Tag', related_name="tags")
    state = models.CharField(max_length=32, blank=True, null=True)
    override_bucket = models.CharField(max_length=255, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now = True)

    @property
    def bucket(self):
        return self.override_bucket or "lynx-dataset-"+self.name+'-'+str(self.id)

    class Meta:
        db_table = 'datasets'

class DataSource(models.Model):
    name = models.CharField(max_length=255)
    dataset = models.ForeignKey('Dataset', on_delete=models.DO_NOTHING, related_name="data_sources")
    type = models.CharField(null=True, blank=True, max_length=32)
    about = models.TextField(null=True, blank=True, max_length=2048)
    columns = JSONField(null = True, blank = True, default = None)
    preview = JSONField(null = True, blank = True, default = None)

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