# Generated by Django 2.2.1 on 2019-06-26 12:44

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('mainapp', '0012_auto_20190626_1526'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dataset',
            name='name',
            field=models.CharField(max_length=255),
        ),
    ]