from django.test import TestCase
from rest_framework.test import APIRequestFactory

from mainapp.models import User, Organization, Dataset, Tag, DataSource
from mainapp.views import AWSHealthCheck, DatasetViewSet
from django.test import Client


class UserTest(TestCase):
    admin_user_email = "admin_user@lynx.com"
    agg_user_email = "agg_user@lynx.com"

    def create_admin_user(self):
        admin_user = User.objects.create(
            email=self.admin_user_email,
            is_active=True,
            is_superuser=True,
            is_admin=True,
            name="Lynx",
            first_login=False,
            organization=self.organization,
            cognito_id="1234",
            is_execution=True,
        )
        return admin_user

    def create_agg_user(self):
        agg_user = User.objects.create(
            email=self.agg_user_email,
            is_active=True,
            is_superuser=True,
            is_admin=False,
            name="Lynx",
            first_login=False,
            organization=self.organization,
            cognito_id="1324",
            is_execution=True,
        )
        return agg_user

    def create_hidden_dataset(self, admin_user, organization, tag):
        hidden_dataset = Dataset.objects.create(
            name="Private Hidden Dataset",
            description="...",
            readme=None,
            user_created=admin_user,
            state="private",
            is_discoverable=False,
            organization=organization,
        )
        hidden_dataset.tags.set(Tag.objects.filter(id__in=[x.id for x in [tag]]))
        hidden_dataset.admin_users.set(
            User.objects.filter(id__in=[x.id for x in [admin_user]])
        )
        return hidden_dataset

    def create_datasource(self, dataset):
        return DataSource.objects.create(name="A test DataSource", dataset=dataset)

    def create_dataset_tag(self):
        tag = Tag.objects.create(name="Test Tag")
        return tag

    def create_organization(self):
        organization = Organization.objects.create(name="Lynx", logo=None)
        return organization

    def setUp(self):
        self.organization = self.create_organization()
        self.admin_user = self.create_admin_user()
        self.agg_user = self.create_agg_user()
        self.client = Client()
        self.client.force_login(self.admin_user)

    def test_is_admin_user(self):
        admin_user = User.objects.get(email=self.admin_user_email)
        self.assertTrue(admin_user.is_admin)

    def test_user_has_data_sources(self):
        admin_user = User.objects.get(email=self.admin_user_email)
        agg_user = User.objects.get(email=self.agg_user_email)
        tag = self.create_dataset_tag()
        organization = self.create_organization()
        admin_dataset = self.create_hidden_dataset(admin_user, organization, tag)
        agg_dataset = self.create_hidden_dataset(agg_user, organization, tag)
        self.create_datasource(admin_dataset)
        self.create_datasource(agg_dataset)

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
        tag = self.create_dataset_tag()
        organization = self.create_organization()
        self.create_hidden_dataset(admin_user, organization, tag)
        self.create_hidden_dataset(agg_user, organization, tag)

        self.assertTrue(admin_user.datasets)
        self.assertTrue(agg_user.datasets)

    def test_user_is_staff(self):
        admin_user = User.objects.get(email=self.admin_user_email)

        self.assertTrue(admin_user.is_staff)

    def test_is_not_agg_user(self):
        agg_user = User.objects.get(email=self.agg_user_email)

        self.assertFalse(agg_user.is_admin)

    def test_is_dataset_hidden(self):
        random_user = User.objects.get(email=self.agg_user_email)
        admin_user = User.objects.get(email=self.admin_user_email)
        tag = self.create_dataset_tag()
        organization = self.create_organization()
        hidden_dataset = self.create_hidden_dataset(admin_user, organization, tag)

        self.assertTrue(hidden_dataset in admin_user.datasets)
        self.assertFalse(hidden_dataset in random_user.datasets)

    def test_unauthorized_user(self):
        """Accessible health call for un authenticated user"""
        api_request = APIRequestFactory().get("")
        health_view = AWSHealthCheck.as_view()
        response = health_view(api_request)
        self.assertEqual(response.status_code, 200)

    def test_unauthorized_user(self):
        """Unauthenticated user test"""
        api_request = APIRequestFactory().get("")
        dataset_view = DatasetViewSet.as_view({"get": "retrieve"})
        response = dataset_view(api_request)
        self.assertEqual(response.status_code, 401)

    def test_list_users_no_execution(self):
        """Do not return execution users"""
        user = User.objects.create(
            is_active=True,
            is_execution=True,
            email="abcde@b.c",
            organization=self.organization,
        )
        response = self.client.get("/users/", {"query_param": "abc"})

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(user.id, [k["id"] for k in response.data])
        self.assertListEqual(response.data, [])

    def test_list_users_no_inactive(self):
        """Do not return inactive users"""
        user = User.objects.create(
            is_active=False,
            is_execution=False,
            email="abcde@b.c",
            organization=self.organization,
        )
        response = self.client.get("/users/", {"query_param": "abc"})

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(user.id, [k["id"] for k in response.data])
        self.assertListEqual(response.data, [])

    def test_list_users_valid_user(self):
        """Return valid user"""
        user = User.objects.create(
            is_active=True,
            is_execution=False,
            email="abcde@b.c",
            organization=self.organization,
        )
        response = self.client.get("/users/", {"query_param": "abc"})

        self.assertEqual(response.status_code, 200)
        self.assertIn(user.id, [k["id"] for k in response.data])

    def test_list_users_user_not_found(self):
        """Return empty list when no user"""
        response = self.client.get("/users/", {"query_param": "abd"})

        self.assertEqual(response.status_code, 200)
        self.assertListEqual(response.data, [])
