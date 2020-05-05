from django.test import TestCase
from mainapp.models import User, Organization, Dataset, Tag


class DatabaseTest(TestCase):
    admin_user_email = "admin_user@lynx.com"

    def setUp(self):
        self.organization = Organization.objects.create(name="Lynx", logo=None)
        self.admin_user = User.objects.create(
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
        self.dataset_tag = Tag.objects.create(name="Test Tag")
        self.public_dataset = Dataset.objects.create(
            name="Public Dataset",
            description="...",
            readme=None,
            user_created=self.admin_user,
            state="public",
            is_discoverable=True,
            organization=self.organization,
        )
        self.private_dataset_discoverable = Dataset.objects.create(
            name="Private Discoverable Dataset",
            description="...",
            readme=None,
            user_created=self.admin_user,
            state="private",
            is_discoverable=True,
            default_user_permission="aggregated_access",
            organization=self.organization,
        )
        self.private_dataset_not_discoverable = Dataset.objects.create(
            name="Private Hidden Dataset",
            description="...",
            readme=None,
            user_created=self.admin_user,
            state="private",
            is_discoverable=False,
            organization=self.organization,
        )
        self.public_dataset.tags.set(
            Tag.objects.filter(id__in=[x.id for x in [self.dataset_tag]])
        )
        self.private_dataset_discoverable.tags.set(
            Tag.objects.filter(id__in=[x.id for x in [self.dataset_tag]])
        )
        self.private_dataset_not_discoverable.tags.set(
            Tag.objects.filter(id__in=[x.id for x in [self.dataset_tag]])
        )

    def test_public_dataset(self):
        public_dataset = Dataset.objects.get(name=self.public_dataset.name)
        self.assertTrue(public_dataset.is_discoverable)

    def test_private_dataset(self):
        private_dataset_discoverable = Dataset.objects.get(
            name=self.private_dataset_discoverable.name
        )
        private_dataset_not_discoverable = Dataset.objects.get(
            name=self.private_dataset_not_discoverable.name
        )
        self.assertTrue(private_dataset_discoverable.is_discoverable)
        self.assertFalse(private_dataset_not_discoverable.is_discoverable)
