# Generated by Django 2.2.1 on 2020-11-15 10:17

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('mainapp', '0049_s3_objects'),
    ]

    operations = [
        migrations.AddField(
            model_name='user',
            name='agreed_eula_file_path',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
