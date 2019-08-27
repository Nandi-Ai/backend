from django.contrib import admin
from .models import *
from django.contrib.auth.models import Group as authgroup

admin.site.unregister(authgroup)


class TagAdmin(admin.ModelAdmin):
    # fields = ('name', 'category')
    list_display =  ('name', 'category')


admin.site.register(Tag, TagAdmin)
