# Generated by Django 2.2.20 on 2021-06-03 22:07

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('datasets', '0028_dataset_collections'),
        ('collection', '0004_auto_20210601_1631'),
    ]

    operations = [
        migrations.AddField(
            model_name='collection',
            name='datasets',
            field=models.ManyToManyField(to='datasets.Dataset'),
        ),
    ]
