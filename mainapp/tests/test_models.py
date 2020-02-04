import django

django.setup()

from django.test import TestCase
from mainapp.models import User, Organization


class UserTest(TestCase):
    admin_user_email = 'admin_user@lynx.com'
    agg_user_email = 'agg_user@lynx.com'

    def setUp(self):
        self.organization = Organization.objects.create(
            name='Lynx',
            logo=None
        )
        self.admin_user = User.objects.create(
            email=self.admin_user_email,
            is_active=True,
            is_superuser=True,
            is_admin=True,
            name='Lynx',
            first_login=False,
            organization=self.organization,
            cognito_id='1234',
            is_execution=True
        )
        self.agg_user = User.objects.create(
            email=self.agg_user_email,
            is_active=True,
            is_superuser=True,
            is_admin=False,
            name='Lynx',
            first_login=False,
            organization=self.organization,
            cognito_id='1234',
            is_execution=True
        )

    def test_is_admin_user(self):
        admin_user = User.objects.get(email=self.admin_user_email)
        self.assertTrue(admin_user.is_admin)

    def test_user_has_data_sources(self):
        admin_user = User.objects.get(email=self.admin_user_email)
        agg_user = User.objects.get(email=self.agg_user_email)

        self.assertTrue(admin_user.data_sources)
        self.assertTrue(agg_user.data_sources)

    def test_user_has_related_studies(self):
        admin_user = User.objects.get(email=self.admin_user_email)
        agg_user = User.objects.get(email=self.agg_user_email)

        self.assertFalse(admin_user.related_studies)
        self.assertFalse(agg_user.related_studies)

    def test_user_has_dataset(self):
        admin_user = User.objects.get(email=self.admin_user_email)
        agg_user = User.objects.get(email=self.agg_user_email)

        self.assertTrue(admin_user.datasets)
        self.assertTrue(agg_user.datasets)

    def test_user_is_staff(self):
        admin_user = User.objects.get(email=self.admin_user_email)

        self.assertTrue(admin_user.is_staff)

    def test_is_not_agg_user(self):
        agg_user = User.objects.get(email=self.agg_user_email)

        self.assertFalse(agg_user.is_admin)
