"""mainapp URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.conf.urls import url, include
from django.contrib import admin
from django.urls import path
from rest_framework.routers import SimpleRouter

from mainapp import views
from mainapp.views import schema_view


# from django.contrib.auth import views as auth_views


class OptionalSlashRouter(SimpleRouter):
    def __init__(self):
        self.trailing_slash = "/?"
        super(SimpleRouter, self).__init__()


router = OptionalSlashRouter()

router.register(r"users", views.UserViewSet, "users")
router.register(r"organizations", views.OrganizationViewSet, "organizations")
router.register(r"tags", views.TagViewSet, "tags")
router.register(r"datasets", views.DatasetViewSet, "datasets")
router.register(r"data_sources", views.DataSourceViewSet, "data_sources")
router.register(r"studies", views.StudyViewSet, "studies")
router.register(r"activities", views.ActivityViewSet, "activities")
router.register(r"requests", views.RequestViewSet, "requests")
router.register(r"my_requests", views.MyRequestsViewSet, "my_requests")
router.register(r"documentation", views.DocumentationViewSet, "documentation")

urlpatterns = [
    url("", include(router.urls)),
    path("api-auth/", include("rest_framework.urls", namespace="rest_framework")),
    path("admin/", admin.site.urls),
    url(r"^docs/", schema_view),
    url(
        r"^health_check_aws/?$", views.AWSHealthCheck.as_view(), name="health_check_aws"
    ),
    url(r"^me/?$", views.CurrentUserView.as_view(), name="me"),
    url(
        r"^get_dataset_sts/(?P<dataset_id>[^/]+)/?$",
        views.GetDatasetSTS.as_view(),
        name="get_dataset_sts",
    ),  # for frontend
    url(
        r"^get_execution/?$", views.GetExecution.as_view(), name="get_execution"
    ),  # for frontend
    url(
        r"^get_execution_user/$",
        views.GetExecutionUser.as_view(),
        name="get_execution_user",
    ),
    url(
        r"^get_execution_config/?$",
        views.GetExecutionConfig.as_view(),
        name="get_execution_config",
    ),  # for execution
    url(r"^dummy/?$", views.Dummy.as_view(), name="dummy"),
    url(
        r"^get_static_sts/(?P<file_name>[^/]*)/?$",
        views.GetStaticSTS.as_view(),
        name="get_static_sts",
    ),  # for uploading static images
    url(r"^run_query/?$", views.RunQuery.as_view(), name="run_query"),  # for execution
    url(r"^create_cohort/?$", views.CreateCohort.as_view(), name="create_cohort"),
    url(r"^query/?$", views.Query.as_view(), name="query"),
    url(r"^challenges/?$", views.QuickSightChallenges.as_view(), name="quicksight"),
    url(
        r"^dashboards/?$",
        views.QuickSightActivitiesDashboard.as_view(),
        name="quicksight",
    ),
    url(
        r"^dashboards_clalit/?$",
        views.QuickSightActivitiesClalitDashboard.as_view(),
        name="quicksight",
    ),
    url(r"^versions/?$", views.Version.as_view(), name="versions"),
    url(
        r"^requests/respond/(?P<user_request_id>[^/]+)/?$",
        views.HandleDatasetAccessRequest.as_view(),
        name="respond_request",
    ),
    url(r"^monitoring/?$", views.Monitoring.as_view(), name="monitoring"),
    url(r"^study_status/?$", views.UpdateStudyStatus.as_view(), name="study_status"),
    url(r"^register_study/?$", views.RegisterStudy.as_view(), name="register_study"),
]
