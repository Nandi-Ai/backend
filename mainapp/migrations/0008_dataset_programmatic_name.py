# Generated by Django 2.2.1 on 2019-07-29 11:00

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('mainapp', '0007_auto_20190728_1452'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='programmatic_name',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]