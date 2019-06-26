# Generated by Django 2.2.1 on 2019-06-25 13:29

import django.contrib.postgres.fields.jsonb
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('mainapp', '0009_auto_20190620_1049'),
    ]

    operations = [
        migrations.CreateModel(
            name='DataSource',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=255)),
                ('type', models.CharField(blank=True, max_length=32, null=True)),
                ('about', models.TextField(blank=True, max_length=2048, null=True)),
                ('columns', django.contrib.postgres.fields.jsonb.JSONField(blank=True, default=None, null=True)),
                ('preview', django.contrib.postgres.fields.jsonb.JSONField(blank=True, default=None, null=True)),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, related_name='data_sources', to='mainapp.Dataset')),
            ],
            options={
                'db_table': 'data_sources',
                'unique_together': {('name', 'dataset')},
            },
        ),
    ]