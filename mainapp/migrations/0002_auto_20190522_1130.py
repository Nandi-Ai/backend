# Generated by Django 2.2.1 on 2019-05-22 08:30

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('mainapp', '0001_initial'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='study',
            unique_together={('name', 'organization')},
        ),
    ]