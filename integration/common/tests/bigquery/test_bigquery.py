# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
from unittest.mock import MagicMock

from openlineage.client.facet import ExternalQueryRunFacet
from openlineage.common.dataset import Dataset, Field, Source
from openlineage.common.provider.bigquery import BigQueryDatasetsProvider, BigQueryJobRunFacet


def read_file_json(file):
    with open(file=file, mode='r') as f:
        return json.loads(f.read())


class TableMock(MagicMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.inputs = [
            read_file_json('tests/bigquery/table_details.json'),
            read_file_json("tests/bigquery/out_table_details.json")
        ]

    @property
    def _properties(self):
        return self.inputs.pop()


def test_bq_job_information():
    job_details = read_file_json('tests/bigquery/job_details.json')
    client = MagicMock()
    client.get_job.return_value._properties = job_details

    client.get_table.return_value = TableMock()

    statistics = BigQueryDatasetsProvider(client=client).get_facets("job_id")

    assert statistics.run_facets == {
        'bigQuery_job': BigQueryJobRunFacet(
            cached=False,
            billedBytes=111149056,
            properties=json.dumps(job_details)
        ),
        'externalQuery': ExternalQueryRunFacet(
            externalQueryId='job_id',
            source='bigquery'
        )
    }
    assert statistics.inputs == [
        Dataset(
            source=Source(
                scheme='bigquery',
                connection_url='bigquery'
            ),
            name='bigquery-public-data.usa_names.usa_1910_2013',
            fields=[
                Field('state', 'STRING', [], '2-digit state code'),
                Field('gender', 'STRING', [], 'Sex (M=male or F=female)'),
                Field('year', 'INTEGER', [], '4-digit year of birth'),
                Field('name', 'STRING', [], 'Given name of a person at birth'),
                Field('number', 'INTEGER', [], 'Number of occurrences of the name')
            ]
        )
    ]
    assert statistics.output == Dataset(
        source=Source(
            scheme='bigquery',
            connection_url='bigquery'
        ),
        name='bq-airflow-openlineage.new_dataset.output_table',
    )
