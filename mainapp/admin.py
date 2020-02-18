from django.contrib import admin
from django.contrib.auth.models import Group as authgroup

from mainapp.models import Tag

admin.site.unregister(authgroup)


class TagAdmin(admin.ModelAdmin):
    # fields = ('name', 'category')
    list_display = ('name', 'category')


admin.site.register(Tag, TagAdmin)
