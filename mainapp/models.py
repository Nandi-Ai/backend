from django.contrib.auth.models import (BaseUserManager, AbstractBaseUser)
from django.contrib.auth.models import PermissionsMixin
from django.db import models

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
    name = models.CharField(max_length=255, primary_key=True)

    class Meta:
        db_table = 'organizations'

class Study(models.Model):
    name = models.CharField(max_length=255)
    organization = models.ForeignKey("Organization", on_delete=models.DO_NOTHING, related_name="studies")
    datasets = models.ManyToManyField('Dataset', related_name="studies")
    users = models.ManyToManyField('User', related_name="studies")
    execution = models.ForeignKey("Execution", on_delete=models.DO_NOTHING, related_name="studies", null=True)

    class Meta:
        db_table = 'studies'
        unique_together = (("name", "organization"),)

class Dataset(models.Model):
    name = models.CharField(max_length=255)
    users = models.ManyToManyField('User', related_name="datasets")
    #studies m2m

    class Meta:
        db_table = 'datasets'


class Execution(models.Model):
    identifier = models.CharField(max_length=255, null=True)
    user = models.ForeignKey('User', on_delete=models.DO_NOTHING, related_name="executions", null=True)
    # study = models.ForeignKey('Study', on_delete=models.DO_NOTHING, related_name="executions", null=True)

    class Meta:
        db_table = 'executions'