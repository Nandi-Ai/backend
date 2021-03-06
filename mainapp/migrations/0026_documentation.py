# Generated by Django 2.2.1 on 2019-12-28 14:40

from django.db import migrations, models
import django.db.models.deletion
import uuid


class Migration(migrations.Migration):

    dependencies = [
        ('mainapp', '0025_auto_20191212_1412'),
    ]

    operations = [
        migrations.CreateModel(
            name='Documentation',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('file_name', models.CharField(default=None, max_length=255)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('dataset', models.ForeignKey(max_length=255, on_delete=django.db.models.deletion.CASCADE, related_name='documentation', to='mainapp.Dataset')),
            ],
            options={
                'db_table': 'documentations',
            },
        ),
    ]
