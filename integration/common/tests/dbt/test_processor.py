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
        (
            "query_id",
            Adapter.FABRIC_WAREHOUSE,
            "fabric-warehouse://myworkspace.datawarehouse.fabric.microsoft.com",
            {"server": "myworkspace.datawarehouse.fabric.microsoft.com"},
        ),  # Microsoft Fabric Warehouse
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


def test_fabric_warehouse_namespace_with_port(dbt_artifact_processor):
    dbt_artifact_processor.adapter_type = Adapter.FABRIC_WAREHOUSE
    dbt_artifact_processor.extract_dataset_namespace(
        {"server": "myworkspace.datawarehouse.fabric.microsoft.com", "port": 1433}
    )

    assert (
        dbt_artifact_processor.dataset_namespace
        == "fabric-warehouse://myworkspace.datawarehouse.fabric.microsoft.com:1433"
    )


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


class TestParseSingularTests:
    """Tests for singular test handling in parse_assertions (no test_metadata)."""

    @pytest.fixture
    def processor(self):
        processor = DbtArtifactProcessor(
            producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
            job_namespace="test-namespace",
        )
        processor.manifest_version = 11  # Use version < 12 for test_metadata path
        return processor

    def test_singular_test_no_test_metadata(self, processor):
        """Singular tests (manifest v<12) have no test_metadata; name comes from node name."""
        nodes = {
            "test.project.assert_no_future_dates": {
                "name": "assert_no_future_dates",
                # No test_metadata key
            }
        }
        manifest = {
            "parent_map": {
                "test.project.assert_no_future_dates": ["model.project.my_model"],
            }
        }
        run_results = {
            "results": [
                {
                    "unique_id": "test.project.assert_no_future_dates",
                    "status": "pass",
                }
            ]
        }
        context = DbtRunContext(manifest=manifest, run_results=run_results)

        assertions = processor.parse_assertions(context, nodes)

        assert "model.project.my_model" in assertions
        assert len(assertions["model.project.my_model"]) == 1
        assertion = assertions["model.project.my_model"][0]
        assert assertion.assertion == "assert_no_future_dates"
        assert assertion.success is True
        assert assertion.column is None


class TestParseFailures:
    """Tests for failure-count extraction in parse_assertions (generic tests)."""

    @pytest.fixture
    def processor(self):
        processor = DbtArtifactProcessor(
            producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
            job_namespace="test-namespace",
        )
        processor.manifest_version = 11  # Use version < 12 for test_metadata path
        return processor

    def test_warning_test_with_nine_failures(self, processor):
        """Mirrors the dbt-test-env scenario: accepted_values test with severity=warn, failures=9."""
        nodes = {
            "test.project.accepted_values_country": {
                "test_metadata": {
                    "name": "accepted_values",
                    "kwargs": {"column_name": "country", "values": ["USA", "Canada"]},
                },
                "config": {"severity": "warn"},
            }
        }
        manifest = {
            "parent_map": {
                "test.project.accepted_values_country": ["model.project.stg_customers"],
            }
        }
        run_results = {
            "results": [
                {
                    "unique_id": "test.project.accepted_values_country",
                    "status": "warn",
                    "failures": 9,
                }
            ]
        }
        context = DbtRunContext(manifest=manifest, run_results=run_results)

        assertion = processor.parse_assertions(context, nodes)["model.project.stg_customers"][0]

        assert assertion.success is False  # status="warn" is not "pass"
        assert assertion.actual == "9"
        assert assertion.expected == "0"

    def test_passing_test_surfaces_zero_failures(self, processor):
        """Passing tests still report the count; surfacing 0 makes the metric uniformly queryable."""
        nodes = {
            "test.project.unique_id": {
                "test_metadata": {"name": "unique", "kwargs": {"column_name": "id"}},
                "config": {"severity": "error"},
            }
        }
        manifest = {"parent_map": {"test.project.unique_id": ["model.project.my_model"]}}
        run_results = {"results": [{"unique_id": "test.project.unique_id", "status": "pass", "failures": 0}]}
        context = DbtRunContext(manifest=manifest, run_results=run_results)

        assertion = processor.parse_assertions(context, nodes)["model.project.my_model"][0]

        assert assertion.success is True
        assert assertion.actual == "0"
        assert assertion.expected == "0"

    def test_custom_threshold_carried_through_to_expected(self, processor):
        """When error_if/warn_if is non-default, the threshold expression is preserved in `expected`."""
        nodes = {
            "test.project.row_count": {
                "test_metadata": {"name": "row_count", "kwargs": {}},
                "config": {"severity": "warn", "warn_if": "> 100"},
            }
        }
        manifest = {"parent_map": {"test.project.row_count": ["model.project.my_model"]}}
        run_results = {"results": [{"unique_id": "test.project.row_count", "status": "pass", "failures": 50}]}
        context = DbtRunContext(manifest=manifest, run_results=run_results)

        assertion = processor.parse_assertions(context, nodes)["model.project.my_model"][0]

        assert assertion.actual == "50"
        assert assertion.expected == "> 100"

    def test_missing_failures_field_falls_back_to_none(self, processor):
        """Older dbt versions or runtime errors may not report failures; both fields stay None."""
        nodes = {
            "test.project.unique_id": {
                "test_metadata": {"name": "unique", "kwargs": {"column_name": "id"}},
                "config": {"severity": "error"},
            }
        }
        manifest = {"parent_map": {"test.project.unique_id": ["model.project.my_model"]}}
        run_results = {
            "results": [
                {"unique_id": "test.project.unique_id", "status": "pass"}  # no "failures" key
            ]
        }
        context = DbtRunContext(manifest=manifest, run_results=run_results)

        assertion = processor.parse_assertions(context, nodes)["model.project.my_model"][0]

        assert assertion.actual is None
        assert assertion.expected is None


class TestAggregateTestEventStatus:
    """Per-model aggregate test job emits FAIL when an error-severity assertion failed,
    and COMPLETE when all assertions passed or only warn-severity ones failed.

    Regression: legacy ``processor.parse_test`` previously hardcoded status="success",
    so failing tests were emitted as COMPLETE."""

    @pytest.fixture
    def processor(self):
        p = DbtArtifactProcessor(
            producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
            job_namespace="ns",
        )
        p.manifest_version = 11
        p.command = "test"
        p.dataset_namespace = "snowflake://acct"
        p.dbt_run_run_facet = MagicMock(return_value={})
        return p

    @staticmethod
    def _ctx(test_results):
        manifest = {
            "nodes": {
                "model.project.my_model": {
                    "database": "DB",
                    "schema": "SCH",
                    "alias": "my_model",
                    "unique_id": "model.project.my_model",
                    "columns": {},
                },
            },
            "sources": {},
            "parent_map": {
                f"test.project.t{i}": ["model.project.my_model"] for i in range(len(test_results))
            },
        }
        run_results = {"results": test_results}
        nodes = {
            f"test.project.t{i}": {
                "name": f"t{i}",
                "test_metadata": {"name": f"t{i}", "kwargs": {"column_name": "id"}},
                "config": {"severity": severity},
            }
            for i, (severity, _status) in enumerate([(r["_severity"], r["status"]) for r in test_results])
        }
        # remove the synthetic _severity key from results so they look like real run_results
        for r in test_results:
            r.pop("_severity", None)
        return DbtRunContext(manifest=manifest, run_results=run_results, catalog=None), nodes

    def _event_types(self, processor, test_results):
        ctx, nodes = self._ctx(test_results)
        events = processor.parse_test(ctx, nodes)
        return [e.eventType.value for e in events.starts + events.completes + events.fails]

    def test_all_pass_emits_complete(self, processor):
        results = [
            {"unique_id": "test.project.t0", "status": "pass", "_severity": "error"},
        ]
        assert self._event_types(processor, results) == ["START", "COMPLETE"]

    def test_error_severity_failure_emits_fail(self, processor):
        results = [
            {"unique_id": "test.project.t0", "status": "fail", "failures": 3, "_severity": "error"},
        ]
        assert self._event_types(processor, results) == ["START", "FAIL"]

    def test_warn_severity_failure_emits_complete(self, processor):
        # dbt itself doesn't fail the run on warn-severity test failures, so the
        # aggregate test job stays COMPLETE — mirroring CommandCompleted.success=true.
        results = [
            {"unique_id": "test.project.t0", "status": "warn", "failures": 3, "_severity": "warn"},
        ]
        assert self._event_types(processor, results) == ["START", "COMPLETE"]

    def test_mixed_pass_and_warn_failure_emits_complete(self, processor):
        results = [
            {"unique_id": "test.project.t0", "status": "pass", "_severity": "error"},
            {"unique_id": "test.project.t1", "status": "warn", "failures": 1, "_severity": "warn"},
        ]
        assert self._event_types(processor, results) == ["START", "COMPLETE"]

    def test_mixed_error_and_warn_failure_emits_fail(self, processor):
        results = [
            {"unique_id": "test.project.t0", "status": "fail", "failures": 1, "_severity": "error"},
            {"unique_id": "test.project.t1", "status": "warn", "failures": 1, "_severity": "warn"},
        ]
        assert self._event_types(processor, results) == ["START", "FAIL"]
