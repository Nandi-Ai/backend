# Generated by Django 2.2.1 on 2019-06-26 11:52

from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('mainapp', '0010_datasource'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dataset',
            name='users',
            field=models.ManyToManyField(null=True, related_name='datasets', to=settings.AUTH_USER_MODEL),
        ),
    ]
