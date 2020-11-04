from rest_framework.permissions import IsAuthenticated


class IsDatasetAdmin(IsAuthenticated):
    def has_object_permission(self, request, view, obj):
        return request.user.permission(obj) == "admin"


class IsDataSourceAdmin(IsDatasetAdmin):
    def has_object_permission(self, request, view, obj):
        return super().has_object_permission(request, view, obj.dataset)


class IsMethodAdmin(IsDatasetAdmin):
    def has_object_permission(self, request, view, obj):
        return super().has_object_permission(request, view, obj.dataset)


class IsStudyAdmin(IsAuthenticated):
    def has_object_permission(self, request, view, obj):
        return request.user in obj.users.all()


class IsStudyExecution(IsStudyAdmin):
    def has_object_permission(self, request, view, obj):
        execution = request.user.the_execution.last()
        return obj.execution.id == execution.id
