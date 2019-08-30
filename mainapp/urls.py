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
from django.contrib import admin
from django.urls import path
from django.conf.urls import url, include
from mainapp import views
from mainapp.views import schema_view
from rest_framework.routers import SimpleRouter
from mainapp.lib import startup

# from django.contrib.auth import views as auth_views

class OptionalSlashRouter(SimpleRouter):

    def __init__(self):
        self.trailing_slash = '/?'
        super(SimpleRouter, self).__init__()
#

router = OptionalSlashRouter()

router.register(r'users', views.UserViewSet, 'users')
router.register(r'tags', views.TagViewSet, 'tags')
router.register(r'datasets', views.DatasetViewSet, 'datasets')
router.register(r'data_sources', views.DataSourceViewSet, 'data_sources')
router.register(r'studies', views.StudyViewSet, 'studies')
router.register(r'activities', views.ActivityViewSet, 'activities')
router.register(r'requests', views.RequestViewSet, 'requests')
router.register(r'my_requests', views.MyRequestsViewSet, 'my_requests')

urlpatterns = [
    url('', include(router.urls)),
    path('api-auth/', include('rest_framework.urls', namespace='rest_framework')),
    path('admin/', admin.site.urls),
    url(r'^docs/', schema_view),
    url(r'^me/?$', views.CurrentUserView.as_view(), name='me'),
    url(r'^get_dataset_sts/(?P<dataset_id>[^/]+)/?$', views.GetDatasetSTS.as_view(), name='get_dataset_sts'), #for fronted
    url(r'^get_execution/?$', views.GetExecution.as_view(), name='get_execution'), #for forntend
    url(r'^get_execution_config/?$', views.GetExecutionConfig.as_view(), name='get_execution_config'), #for execution
    url(r'^dummy/?$', views.Dummy.as_view(), name='dummy'),
    url(r'^get_sts/?$', views.GetSTS.as_view(), name='get_sts'), #for execution
    url(r'^send_sync_signal/?$', views.SendSyncSignal.as_view(), name='send_sync_signal'), #for execution
    url(r'^run_query/?$', views.RunQuery.as_view(), name='run_query'), #for execution
    url(r'^requests/respond/(?P<user_request_id>[^/]+)/?$', views.HandleDatasetAccessRequest.as_view(),
        name='respond_request'),
]

startup()
