from unittest.mock import patch
from django.test import TestCase

from mainapp.exceptions import InvalidOrganizationSettings, InvalidOrganizationOrgValues
from mainapp.utils.decorators import organization_dependent


class QueryViewTestCase(TestCase):
    @patch("mainapp.utils.decorators.settings")
    def test_decorator_success(self, settings_mock):
        data = {
            "ACCOUNT_NUMBER": "12345",
            "AWS_ACCESS_KEY_ID": "some_AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY": "some_AWS_SECRET_ACCESS_KEY",
            "AWS_REGION": "some_region",
            "AWS_GLUE_SERVICE_ROLE": "some_AWS_GLUE_SERVICE_ROLE",
        }
        settings_mock.ORG_VALUES = {"health_org": data}

        @organization_dependent
        def func_for_decorator(org_settings, org_name):
            return org_settings, org_name

        result_org_setting, result_org_name = func_for_decorator(org_name="health_org")
        self.assertEqual("health_org", result_org_name)
        self.assertEqual(data, result_org_setting)

    @patch("mainapp.utils.decorators.settings")
    def test_decorator_missing_settings(self, settings_mock):
        data = {
            "ACCOUNT_NUMBER": "12345",
            "AWS_ACCESS_KEY_ID": "some_AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY": "some_AWS_SECRET_ACCESS_KEY",
            "AWS_REGION": "some_region",
            "AWS_GLUE_SERVICE_ROLE": "some_AWS_GLUE_SERVICE_ROLE",
        }
        settings_mock.ORG_VALUES = {"health_org": data}

        @organization_dependent
        def func_for_decorator(org_settings, org_name):
            return org_settings, org_name

        self.assertRaises(
            InvalidOrganizationSettings, func_for_decorator, org_name="other_health_org"
        )

    @patch("mainapp.utils.decorators.settings")
    def test_decorator_missing_settings_org_value(self, settings_mock):
        del settings_mock.ORG_VALUES

        @organization_dependent
        def func_for_decorator(org_settings, org_name):
            return org_settings, org_name

        self.assertRaises(
            InvalidOrganizationOrgValues,
            func_for_decorator,
            org_name="other_health_org",
        )
