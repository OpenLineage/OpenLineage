# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
from unittest.mock import MagicMock

from openlineage.common.dataset import Dataset, Field, Source
from openlineage.common.models import DbColumn, DbTableSchema
from openlineage.common.provider.redshift_data import RedshiftDataDatasetsProvider
from openlineage.common.sql import DbTableMeta

REDSHIFT_DATABASE = "dev"
REDSHIFT_DATABASE_USER = "admin"
CLUSTER_IDENTIFIER = "redshift-cluster-1"
REGION_NAME = "eu-west-2"

REDSHIFT_QUERY = """
CREATE TABLE IF NOT EXISTS fruit (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
            );
            """
POLL_INTERVAL = 10

DB_NAME = "dev"
DB_SCHEMA_NAME = "public"
DB_TABLE_NAME = DbTableMeta("fruit")
DB_TABLE_COLUMNS = [
    DbColumn(name="fruit_id", type="int4", ordinal_position=1),
    DbColumn(name="name", type="varchar", ordinal_position=2),
    DbColumn(name="color", type="varchar", ordinal_position=3),
]
DB_TABLE_SCHEMA = DbTableSchema(
    schema_name=DB_SCHEMA_NAME, table_name=DB_TABLE_NAME, columns=DB_TABLE_COLUMNS
)


def read_file_json(file):
    with open(file=file, mode="r") as f:
        return json.loads(f.read())


def test_redshift_get_facets():
    client = MagicMock()
    client.describe_statement.return_value = read_file_json(
        "tests/redshift_data/statement_details.json"
    )
    client.describe_table.return_value = read_file_json(
        "tests/redshift_data/table_details.json"
    )
    inputs = [DbTableMeta(f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}")]
    connection_details = dict(
        database=REDSHIFT_DATABASE,
        db_user=REDSHIFT_DATABASE_USER,
        cluster_identifier=CLUSTER_IDENTIFIER,
        region=REGION_NAME,
    )
    provider = RedshiftDataDatasetsProvider(
        client=client, connection_details=connection_details
    )

    facets = provider.get_facets("job_id", inputs=inputs, outputs=[])

    source = Source(
        scheme="redshift",
        authority=f"{CLUSTER_IDENTIFIER}.{REGION_NAME}:5439",
    )
    expected_inputs = [
        Dataset(
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
            source=source,
            fields=[Field.from_column(column) for column in DB_TABLE_COLUMNS],
        )
    ]

    assert facets.inputs == expected_inputs
