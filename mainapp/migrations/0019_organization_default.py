# Generated by Django 2.2.1 on 2019-09-15 15:44

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('mainapp', '0018_cover'),
    ]

    operations = [
        migrations.AddField(
            model_name='organization',
            name='default',
            field=models.BooleanField(default=False),
        ),
    ]
