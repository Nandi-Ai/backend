# Generated by Django 2.2.1 on 2019-08-26 09:56

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('mainapp', '0013_remove_activity_action'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='organization',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='datasets', to='mainapp.Organization'),
        ),
        migrations.AlterField(
            model_name='dataset',
            name='default_user_permission',
            field=models.CharField(choices=[('none', 'none'), ('aggregated_access', 'aggregated_access')],
                                   max_length=32, null=True),
        ),
    ]
