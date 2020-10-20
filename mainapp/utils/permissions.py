from rest_framework.permissions import IsAuthenticated


class IsDatasetAdmin(IsAuthenticated):
    def has_object_permission(self, request, view, obj):
        if view.action in view.ADMIN_PROTECTED_ENDPOINTS:
            return request.user.permission(obj) == "admin"

        return True


class IsDataSourceAdmin(IsDatasetAdmin):
    def has_object_permission(self, request, view, obj):
        return super().has_object_permission(request, view, obj.dataset)
