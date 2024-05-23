# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
from unittest.mock import MagicMock

import pytest
from openlineage.client.facet_v2 import external_query_run
from openlineage.common.dataset import Dataset, Field, Source
from openlineage.common.provider.bigquery import (
    BigQueryDatasetsProvider,
    BigQueryJobRunFacet,
    BigQueryStatisticsDatasetFacet,
)


def read_file_json(file):
    with open(file=file) as f:
        return json.loads(f.read())


class TableMock(MagicMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.inputs = [
            read_file_json("tests/bigquery/table_details.json"),
            read_file_json("tests/bigquery/out_table_details.json"),
        ]

    @property
    def _properties(self):
        return self.inputs.pop()


def test_bq_job_information():
    job_details = read_file_json("tests/bigquery/job_details.json")
    client = MagicMock()
    client.get_job.return_value._properties = job_details

    client.get_table.return_value = TableMock()

    statistics = BigQueryDatasetsProvider(client=client).get_facets("job_id")

    job_details["configuration"]["query"].pop("query")
    assert statistics.run_facets == {
        "bigQuery_job": BigQueryJobRunFacet(
            cached=False, billedBytes=111149056, properties=json.dumps(job_details)
        ),
        "externalQuery": external_query_run.ExternalQueryRunFacet(
            externalQueryId="job_id", source="bigquery"
        ),
    }
    assert statistics.inputs == [
        Dataset(
            source=Source(scheme="bigquery", connection_url="bigquery"),
            name="bigquery-public-data.usa_names.usa_1910_2013",
            fields=[
                Field("state", "STRING", [], "2-digit state code"),
                Field("gender", "STRING", [], "Sex (M=male or F=female)"),
                Field("year", "INTEGER", [], "4-digit year of birth"),
                Field("name", "STRING", [], "Given name of a person at birth"),
                Field("number", "INTEGER", [], "Number of occurrences of the name"),
            ],
        )
    ]
    assert statistics.output == Dataset(
        source=Source(scheme="bigquery", connection_url="bigquery"),
        name="bq-airflow-openlineage.new_dataset.output_table",
    )
    assert statistics.outputs == [
        Dataset(
            source=Source(scheme="bigquery", connection_url="bigquery"),
            name="bq-airflow-openlineage.new_dataset.output_table",
        )
    ]


def test_bq_script_job_information():
    script_job_details = read_file_json("tests/bigquery/script_job_details.json")
    query_job_details = read_file_json("tests/bigquery/job_details.json")
    client = MagicMock()
    client.get_job.side_effect = [
        MagicMock(_properties=script_job_details),
        MagicMock(_properties=query_job_details),
    ]
    client.list_jobs.return_value = ["child_job_id"]

    client.get_table.return_value = TableMock()

    statistics = BigQueryDatasetsProvider(client=client).get_facets("script_job_id")

    script_job_details["configuration"]["query"].pop("query")
    assert statistics.run_facets == {
        "bigQuery_job": BigQueryJobRunFacet(
            cached=False, billedBytes=120586240, properties=json.dumps(script_job_details)
        ),
        "externalQuery": external_query_run.ExternalQueryRunFacet(
            externalQueryId="script_job_id", source="bigquery"
        ),
    }
    assert statistics.inputs == [
        Dataset(
            source=Source(scheme="bigquery", connection_url="bigquery"),
            name="bigquery-public-data.usa_names.usa_1910_2013",
            fields=[
                Field("state", "STRING", [], "2-digit state code"),
                Field("gender", "STRING", [], "Sex (M=male or F=female)"),
                Field("year", "INTEGER", [], "4-digit year of birth"),
                Field("name", "STRING", [], "Given name of a person at birth"),
                Field("number", "INTEGER", [], "Number of occurrences of the name"),
            ],
        )
    ]
    assert statistics.output == Dataset(
        source=Source(scheme="bigquery", connection_url="bigquery"),
        name="bq-airflow-openlineage.new_dataset.output_table",
    )
    assert statistics.outputs == [
        Dataset(
            source=Source(scheme="bigquery", connection_url="bigquery"),
            name="bq-airflow-openlineage.new_dataset.output_table",
        )
    ]


def test_deduplicate_outputs():
    outputs = [
        None,
        Dataset(
            Source(name="s1"),
            name="d1",
            custom_facets={"stats": BigQueryStatisticsDatasetFacet(1, 2), "test1": "test1"},
            output_facets={"outputStatistics": BigQueryStatisticsDatasetFacet(3, 4)},
        ),
        Dataset(
            Source(name="s4"),
            name="d1",
            custom_facets={"stats": BigQueryStatisticsDatasetFacet(10, 20)},
            output_facets={"outputStatistics": BigQueryStatisticsDatasetFacet(30, 40), "test10": "test10"},
        ),
        Dataset(
            Source(name="s2"),
            name="d2",
            custom_facets={"stats": BigQueryStatisticsDatasetFacet(8, 9)},
            output_facets={"outputStatistics": BigQueryStatisticsDatasetFacet(6, 7), "test20": "test20"},
        ),
        Dataset(
            Source(name="s3"),
            name="d2",
            custom_facets={"stats": BigQueryStatisticsDatasetFacet(80, 90), "test2": "test2"},
            output_facets={"outputStatistics": BigQueryStatisticsDatasetFacet(60, 70)},
        ),
    ]
    result = BigQueryDatasetsProvider._deduplicate_outputs(outputs)
    assert len(result) == 2
    first_result = result[0]
    assert first_result.name == "d1"
    assert not first_result.custom_facets
    assert first_result.output_facets == {"test10": "test10"}
    second_result = result[1]
    assert second_result.name == "d2"
    assert second_result.custom_facets == {"test2": "test2"}
    assert not second_result.output_facets


@pytest.mark.parametrize("cache", (None, "false", False, 0))
def test_get_job_run_facet_no_cache_and_with_bytes(cache):
    properties = {
        "statistics": {"query": {"cacheHit": cache, "totalBytesBilled": 10}},
        "configuration": {"query": {"query": "SELECT ..."}},
    }
    result = BigQueryDatasetsProvider._get_job_run_facet(properties)
    assert result.cached is False
    assert result.billedBytes == 10
    properties["configuration"]["query"].pop("query")
    assert result.properties == json.dumps(properties)


@pytest.mark.parametrize("cache", ("true", True))
def test_get_job_run_facet_with_cache_and_no_bytes(cache):
    properties = {
        "statistics": {
            "query": {
                "cacheHit": cache,
            }
        },
        "configuration": {"query": {"query": "SELECT ..."}},
    }
    result = BigQueryDatasetsProvider._get_job_run_facet(properties)
    assert result.cached is True
    assert result.billedBytes is None
    properties["configuration"]["query"].pop("query")
    assert result.properties == json.dumps(properties)


def test_get_statistics_dataset_facet_no_query_plan():
    properties = {
        "statistics": {"query": {"totalBytesBilled": 10}},
        "configuration": {"query": {"query": "SELECT ..."}},
    }
    result = BigQueryDatasetsProvider._get_statistics_dataset_facet(properties)
    assert result is None


def test_get_statistics_dataset_facet_no_stats():
    properties = {
        "statistics": {"query": {"totalBytesBilled": 10, "queryPlan": [{"test": "test"}]}},
        "configuration": {"query": {"query": "SELECT ..."}},
    }
    result = BigQueryDatasetsProvider._get_statistics_dataset_facet(properties)
    assert result is None


def test_get_statistics_dataset_facet_with_stats():
    properties = {
        "statistics": {
            "query": {
                "totalBytesBilled": 10,
                "queryPlan": [{"recordsWritten": 123, "shuffleOutputBytes": "321"}],
            }
        },
        "configuration": {"query": {"query": "SELECT ..."}},
    }
    result = BigQueryDatasetsProvider._get_statistics_dataset_facet(properties)
    assert result.rowCount == 123
    assert result.size == 321
