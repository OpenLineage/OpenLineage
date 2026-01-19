# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0


from unittest.mock import MagicMock

import pytest
from openlineage.client.facet_v2 import external_query_run, processing_engine_run
from openlineage.client.uuid import generate_new_uuid
from openlineage.common.provider.dbt.facets import DbtRunRunFacet, DbtVersionRunFacet
from openlineage.common.provider.dbt.processor import Adapter, DbtArtifactProcessor, DbtRunContext
from openlineage.common.provider.dbt.utils import __version__ as openlineage_version

DBT_VERSION = "0.0.1"
JOB_NAMESPACE = "job-namespace"
QUERY_ID = "01bd9b35-0614-b50a-0000-6821d44ae3a2"
RUN_ID = str(generate_new_uuid())


@pytest.fixture()
def run_result():
    # This is an "individual" element of the "results" array in the run_results.json file
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
    "adapter_key,adapter_type,dataset_namespace,profile",
    [
        (
            "query_id",
            Adapter.SNOWFLAKE,
            "snowflake://gp12345.us-east-1.aws",
            {"account": "gp12345.us-east-1"},
        ),  # Snowflake
        ("job_id", Adapter.BIGQUERY, "bigquery", {}),  # BigQuery
    ],
)
def test_get_query_id(
    adapter_key, adapter_type, dataset_namespace, profile, dbt_artifact_processor, run_result
):
    run_result["adapter_response"][adapter_key] = QUERY_ID
    dbt_artifact_processor.adapter_type = adapter_type
    dbt_artifact_processor.dbt_run_run_facet = MagicMock()
    dbt_artifact_processor.dbt_run_run_facet.return_value = {
        "dbt_run": DbtRunRunFacet(invocation_id="1", project_name="test", dbt_runtime="core")
    }

    dbt_artifact_processor.extract_dataset_namespace(profile)
    generated_query_id = dbt_artifact_processor.get_query_id(run_result)
    generated_run = dbt_artifact_processor.get_run(run_id=RUN_ID, query_id=generated_query_id)

    assert generated_query_id == QUERY_ID
    assert generated_run.facets == {
        "dbt_version": DbtVersionRunFacet(version=DBT_VERSION),
        "dbt_run": DbtRunRunFacet(invocation_id="1", project_name="test", dbt_runtime="core"),
        "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
            name="dbt",
            version=DBT_VERSION,
            openlineageAdapterVersion=openlineage_version,
        ),
        "externalQuery": external_query_run.ExternalQueryRunFacet(
            externalQueryId=QUERY_ID, source=dataset_namespace
        ),
    }


def test_invalid_adapter(dbt_artifact_processor, run_result):
    run_result["adapter_response"]["query_id"] = None
    dbt_artifact_processor.adapter_type = Adapter.GLUE
    dbt_artifact_processor.dbt_run_run_facet = MagicMock()
    dbt_artifact_processor.dbt_run_run_facet.return_value = {
        "dbt_run": DbtRunRunFacet(invocation_id="1", project_name="test", dbt_runtime="core")
    }
    generated_query_id = dbt_artifact_processor.get_query_id(run_result)
    generated_run = dbt_artifact_processor.get_run(run_id=RUN_ID, query_id=generated_query_id)

    assert generated_query_id is None
    assert generated_run.facets == {
        "dbt_version": DbtVersionRunFacet(version=DBT_VERSION),
        "dbt_run": DbtRunRunFacet(invocation_id="1", project_name="test", dbt_runtime="core"),
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


class TestParseSeverity:
    """Tests for severity extraction in parse_assertions."""

    @pytest.fixture
    def processor(self):
        processor = DbtArtifactProcessor(
            producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
            job_namespace="test-namespace",
        )
        processor.manifest_version = 11  # Use version < 12 for test_metadata path
        return processor

    def test_severity_extracted_and_normalized_to_lowercase(self, processor):
        """Test that severity is extracted from config and normalized to lowercase."""
        nodes = {
            "test.project.test_unique_model_id": {
                "test_metadata": {
                    "name": "unique",
                    "kwargs": {"column_name": "id"},
                },
                "config": {"severity": "ERROR"},
            }
        }
        manifest = {
            "parent_map": {
                "test.project.test_unique_model_id": ["model.project.my_model"],
            }
        }
        run_results = {
            "results": [
                {
                    "unique_id": "test.project.test_unique_model_id",
                    "status": "pass",
                }
            ]
        }
        context = DbtRunContext(manifest=manifest, run_results=run_results)

        assertions = processor.parse_assertions(context, nodes)

        assert "model.project.my_model" in assertions
        assert len(assertions["model.project.my_model"]) == 1
        assertion = assertions["model.project.my_model"][0]
        assert assertion.severity == "error"

    def test_severity_warn_normalized_to_lowercase(self, processor):
        """Test that WARN severity is normalized to lowercase."""
        nodes = {
            "test.project.test_not_null_model_id": {
                "test_metadata": {
                    "name": "not_null",
                    "kwargs": {"column_name": "id"},
                },
                "config": {"severity": "WARN"},
            }
        }
        manifest = {
            "parent_map": {
                "test.project.test_not_null_model_id": ["model.project.my_model"],
            }
        }
        run_results = {
            "results": [
                {
                    "unique_id": "test.project.test_not_null_model_id",
                    "status": "pass",
                }
            ]
        }
        context = DbtRunContext(manifest=manifest, run_results=run_results)

        assertions = processor.parse_assertions(context, nodes)

        assertion = assertions["model.project.my_model"][0]
        assert assertion.severity == "warn"

    def test_severity_none_when_not_in_config(self, processor):
        """Test that severity is None when not present in config."""
        nodes = {
            "test.project.test_unique_model_id": {
                "test_metadata": {
                    "name": "unique",
                    "kwargs": {"column_name": "id"},
                },
                "config": {},  # No severity
            }
        }
        manifest = {
            "parent_map": {
                "test.project.test_unique_model_id": ["model.project.my_model"],
            }
        }
        run_results = {
            "results": [
                {
                    "unique_id": "test.project.test_unique_model_id",
                    "status": "pass",
                }
            ]
        }
        context = DbtRunContext(manifest=manifest, run_results=run_results)

        assertions = processor.parse_assertions(context, nodes)

        assertion = assertions["model.project.my_model"][0]
        assert assertion.severity is None

    def test_severity_none_when_config_missing(self, processor):
        """Test that severity is None when config key is missing entirely."""
        nodes = {
            "test.project.test_unique_model_id": {
                "test_metadata": {
                    "name": "unique",
                    "kwargs": {"column_name": "id"},
                },
                # No config key at all
            }
        }
        manifest = {
            "parent_map": {
                "test.project.test_unique_model_id": ["model.project.my_model"],
            }
        }
        run_results = {
            "results": [
                {
                    "unique_id": "test.project.test_unique_model_id",
                    "status": "pass",
                }
            ]
        }
        context = DbtRunContext(manifest=manifest, run_results=run_results)

        assertions = processor.parse_assertions(context, nodes)

        assertion = assertions["model.project.my_model"][0]
        assert assertion.severity is None
