# Generated by Django 3.2.7 on 2021-09-19 03:55

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('olive', '0001_initial'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='output',
            unique_together={('scenario', 'type', 'idp', 'prod_date', 'owner', 'version', 'simulation')},
        ),
        migrations.AlterUniqueTogether(
            name='output_onelines',
            unique_together={('scenario', 'type', 'idp', 'owner', 'version', 'simulation')},
        ),
    ]
