# Generated by Django 2.2.1 on 2019-05-29 13:03

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('mainapp', '0003_auto_20190526_1220'),
    ]

    operations = [
        migrations.AddField(
            model_name='user',
            name='cognito_id',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]