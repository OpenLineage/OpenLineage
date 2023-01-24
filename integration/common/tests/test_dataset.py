# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import pytest
from openlineage.client.facet import DataSourceDatasetFacet, SchemaDatasetFacet, SchemaField
from openlineage.client.run import Dataset as OpenLineageDataset
from openlineage.common.dataset import Dataset, Source
from openlineage.common.models import DbColumn, DbTableSchema
from openlineage.common.sql import DbTableMeta


@pytest.fixture
def source():
    return Source(
        scheme="dummy",
        authority="localhost:1234",
        connection_url="dummy://localhost:1234"
    )


@pytest.fixture
def table_schema(source):
    schema_name = 'public'
    table_name = DbTableMeta('discounts')
    columns = [
        DbColumn(
            name='id',
            type='int4',
            ordinal_position=1
        ),
        DbColumn(
            name='amount_off',
            type='int4',
            ordinal_position=2
        ),
        DbColumn(
            name='customer_email',
            type='varchar',
            ordinal_position=3
        ),
        DbColumn(
            name='starts_on',
            type='timestamp',
            ordinal_position=4
        ),
        DbColumn(
            name='ends_on',
            type='timestamp',
            ordinal_position=5
        )
    ]
    schema = DbTableSchema(
        schema_name=schema_name,
        table_name=table_name,
        columns=columns
    )
    return source, columns, schema


def test_dataset_from(source):
    dataset = Dataset.from_table(source, 'source_table', 'public')
    assert dataset == Dataset(source=source, name='public.source_table')


def test_dataset_with_db_name(source):
    dataset = Dataset.from_table(source, 'source_table', 'public', 'food_delivery')
    assert dataset == Dataset(source=source, name='food_delivery.public.source_table')


def test_dataset_to_openlineage(table_schema):
    source_name = 'dummy://localhost:1234'
    source, columns, schema = table_schema

    dataset_schema = Dataset.from_table_schema(source, schema)
    assert dataset_schema.to_openlineage_dataset() == OpenLineageDataset(
        namespace=source_name,
        name='public.discounts',
        facets={
            'dataSource': DataSourceDatasetFacet(
                name=source_name,
                uri=source_name
            ),
            'schema': SchemaDatasetFacet(
                fields=[
                    SchemaField(name='id', type='int4'),
                    SchemaField(name='amount_off', type='int4'),
                    SchemaField(name='customer_email', type='varchar'),
                    SchemaField(name='starts_on', type='timestamp'),
                    SchemaField(name='ends_on', type='timestamp')
                ]
            )
        }
    )
