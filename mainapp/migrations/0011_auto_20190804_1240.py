# Generated by Django 2.2.1 on 2019-08-04 12:40

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('mainapp', '0010_datasource_programmatic_name'),
    ]

    operations = [
        migrations.AlterField(
            model_name='activity',
            name='ts',
            field=models.DateTimeField(auto_now_add=True),
        ),
    ]