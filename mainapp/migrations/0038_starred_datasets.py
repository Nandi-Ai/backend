# Generated by Django 2.2.1 on 2020-07-23 17:01

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import uuid


class Migration(migrations.Migration):

    dependencies = [
        ('mainapp', '0037_auto_20200721_1811'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='starred_dataset',
            field=models.ManyToManyField(null=True, related_name='starred_datasets', to=settings.AUTH_USER_MODEL),
        ),
        migrations.CreateModel(
            name='StarredDataset',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='mainapp.Dataset')),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'db_table': 'starred_datasets',
                'unique_together': {('user', 'dataset')},
            },
        ),
    ]