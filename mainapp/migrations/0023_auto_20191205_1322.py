# Generated by Django 2.2.1 on 2019-12-05 13:22

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('mainapp', '0022_auto_20191205_1314'),
    ]

    operations = [
        migrations.RenameField(
            model_name='dataset',
            old_name='bucket',
            new_name='bucket_override',
        ),
    ]