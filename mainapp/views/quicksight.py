import logging

from rest_framework.generics import GenericAPIView
from rest_framework.response import Response

from mainapp import settings
from mainapp.utils import aws_service

logger = logging.getLogger(__name__)


class QuickSightChallenges(GenericAPIView):
    def get(self, request):
        client = aws_service.create_client(
            org_name="Old_SMC", service_name="quicksight"
        )
        data = client.get_dashboard_embed_url(
            AwsAccountId=settings.ORG_VALUES["Old_SMC"]["ACCOUNT_NUMBER"],
            DashboardId=settings.ORG_VALUES["Old_SMC"]["CHALLENGES_DASHBOARD_ID"],
            IdentityType="IAM",
            SessionLifetimeInMinutes=100,
            ResetDisabled=True,
            UndoRedoDisabled=True,
        )
        return Response(data)


class QuickSightActivitiesDashboard(GenericAPIView):
    def get(self, request):
        client = aws_service.create_client(service_name="quicksight")
        data = client.get_dashboard_embed_url(
            AwsAccountId=settings.ORG_VALUES["Lynx MD"]["ACCOUNT_NUMBER"],
            DashboardId=settings.ORG_VALUES["Lynx MD"]["ACTIVITIES_DASHBOARD_ID"],
            IdentityType="IAM",
            SessionLifetimeInMinutes=100,
            ResetDisabled=True,
            UndoRedoDisabled=True,
        )
        return Response(data)
