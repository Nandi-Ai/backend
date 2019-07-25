from django.contrib import admin
from .models import *
from django.contrib.auth.models import Group as authgroup

admin.site.unregister(authgroup)


class TagAdmin(admin.ModelAdmin):
    pass

admin.site.register(Tag, TagAdmin)