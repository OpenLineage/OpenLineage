# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0


import pytest
from openlineage.client.facet_v2 import processing_engine_run
from openlineage.client.generated.external_query_run import ExternalQueryRunFacet
from openlineage.client.uuid import generate_new_uuid
from openlineage.common.provider.dbt.facets import DbtRunRunFacet, DbtVersionRunFacet
from openlineage.common.provider.dbt.processor import Adapter, DbtArtifactProcessor
from openlineage.common.provider.dbt.utils import __version__ as openlineage_version

DBT_VERSION = "0.0.1"
JOB_NAMESPACE = "job-namespace"
QUERY_ID = "01bd9b35-0614-b50a-0000-6821d44ae3a2"
RUN_ID = str(generate_new_uuid())


@pytest.fixture()
def run_result():
    return {
        "status": "pass",
        "timing": [],  # Would include the time of compilation and execution
        "thread_id": None,
        "execution_time": None,
        "adapter_response": {
            "_message": None,
            "code": "SUCCESS",
            "rows_affected": 1,
            # This would be where the query ID would go
        },
        "message": None,
        "failures": 0,
    }


@pytest.fixture()
def dbt_artifact_processor():
    _dbt_artifact_processor = DbtArtifactProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace=JOB_NAMESPACE,
    )
    _dbt_artifact_processor.run_metadata = {"dbt_version": DBT_VERSION, "invocation_id": "1"}

    return _dbt_artifact_processor


@pytest.mark.parametrize(
    "adapter_key,adapter_type", [("query_id", Adapter.SNOWFLAKE), ("job_id", Adapter.BIGQUERY)]
)
def test_get_query_id(adapter_key, adapter_type, dbt_artifact_processor, run_result):
    run_result["adapter_response"][adapter_key] = QUERY_ID
    dbt_artifact_processor.adapter_type = adapter_type
    generated_query_id = dbt_artifact_processor.get_query_id(run_result)
    generated_run = dbt_artifact_processor.get_run(run_id=RUN_ID, query_id=generated_query_id)

    assert generated_query_id == QUERY_ID
    assert generated_run.facets == {
        "dbt_version": DbtVersionRunFacet(version=DBT_VERSION),
        "dbt_run": DbtRunRunFacet(invocation_id="1"),
        "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
            name="dbt",
            version=DBT_VERSION,
            openlineageAdapterVersion=openlineage_version,
        ),
        "externalQuery": ExternalQueryRunFacet(externalQueryId=QUERY_ID, source=JOB_NAMESPACE),
    }


def test_invalid_adapter(dbt_artifact_processor, run_result):
    dbt_artifact_processor.adapter_type = Adapter.DATABRICKS
    generated_query_id = dbt_artifact_processor.get_query_id(run_result)
    generated_run = dbt_artifact_processor.get_run(run_id=RUN_ID, query_id=generated_query_id)

    assert generated_query_id is None
    assert generated_run.facets == {
        "dbt_version": DbtVersionRunFacet(version=DBT_VERSION),
        "dbt_run": DbtRunRunFacet(invocation_id="1"),
        "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
            name="dbt",
            version=DBT_VERSION,
            openlineageAdapterVersion=openlineage_version,
        ),
    }


def test_get_query_id_missing_adapter_response(dbt_artifact_processor, run_result):
    # No adapter_response
    generated_query_id = dbt_artifact_processor.get_query_id(run_result)

    assert generated_query_id is None
