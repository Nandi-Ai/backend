# Generated by Django 2.2.1 on 2020-07-30 11:41

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('mainapp', '0039_remove_null_from_starred'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='is_deleted',
            field=models.BooleanField(default=False),
        ),
    ]