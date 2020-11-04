import logging
from abc import ABC

from botocore.exceptions import ClientError
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response

from mainapp import settings
from mainapp.utils import aws_service
from mainapp.utils.response_handler import ErrorResponse
from mainapp.exceptions import GetDashboardError

logger = logging.getLogger(__name__)


class QuickSightView(GenericAPIView, ABC):
    def _get_dashboard(
        self, account_id, dashboard_id, org_name=settings.LYNX_ORGANIZATION
    ):
        try:
            quicksight_client = aws_service.create_quicksight_client(org_name=org_name)
            data = quicksight_client.get_dashboard_embed_url(
                AwsAccountId=account_id,
                DashboardId=dashboard_id,
                IdentityType="IAM",
                SessionLifetimeInMinutes=100,
                ResetDisabled=True,
                UndoRedoDisabled=True,
            )

            return data
        except ClientError as e:
            raise GetDashboardError(dashboard_id=dashboard_id, error=str(e))

    def _internal_get(self):
        raise NotImplementedError(
            f"{self.__class__.__name__} does not have an implementation for quicksight"
        )

    def get(self, request, *args):
        try:
            return Response(self._internal_get())
        except (NotImplementedError, GetDashboardError) as e:
            return ErrorResponse(e)


class QuickSightChallenges(QuickSightView):
    def _internal_get(self):
        return self._get_dashboard(
            settings.ORG_VALUES["Old_SMC"]["ACCOUNT_NUMBER"],
            settings.ORG_VALUES["Old_SMC"]["CHALLENGES_DASHBOARD_ID"],
            "Old_SMC",
        )


class QuickSightActivitiesDashboard(QuickSightView):
    def _internal_get(self):
        return self._get_dashboard(
            settings.ORG_VALUES[settings.LYNX_ORGANIZATION]["ACCOUNT_NUMBER"],
            settings.ORG_VALUES[settings.LYNX_ORGANIZATION]["ACTIVITIES_DASHBOARD_ID"],
        )


class QuickSightActivitiesClalitDashboard(QuickSightView):
    def _internal_get(self):
        return self._get_dashboard(
            settings.ORG_VALUES[settings.LYNX_ORGANIZATION]["ACCOUNT_NUMBER"],
            settings.ORG_VALUES["Clalit"]["ACTIVITIES_DASHBOARD_ID"],
        )


class QuickSightCNHDashboard(QuickSightView):
    def _internal_get(self):
        return self._get_dashboard(
            settings.ORG_VALUES[settings.LYNX_ORGANIZATION]["ACCOUNT_NUMBER"],
            settings.ORG_VALUES["CNH"]["ACTIVITIES_DASHBOARD_ID"],
        )
