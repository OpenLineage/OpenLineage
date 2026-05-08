# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from unittest import mock

import pytest
from openlineage.common.provider.dbt.utils import CONSUME_STRUCTURED_LOGS_COMMAND_OPTION, PRODUCER
from openlineage.dbt import consume_structured_logs, main


@pytest.mark.parametrize(
    "command_line, number_of_calls",
    [
        # consume dbt structured logs
        (["dbt-ol", CONSUME_STRUCTURED_LOGS_COMMAND_OPTION, "run", "--select", "orders"], 1),
        # consume local artifacts
        (["dbt-ol", "run", "--select", "orders"], 0),
    ],
    ids=[
        "dbt_ol_consumes_structured_logs",
        "dbt_ol_consumes_local_artifacts",
    ],
)
def test_structured_logs(command_line: str, number_of_calls: int, monkeypatch):
    mock_consume_structured_logs = mock.MagicMock()
    mock_consume_local_artifacts = mock.MagicMock()
    monkeypatch.setattr("openlineage.dbt.consume_structured_logs", mock_consume_structured_logs)
    monkeypatch.setattr("openlineage.dbt.consume_local_artifacts", mock_consume_local_artifacts)
    monkeypatch.setattr("sys.argv", command_line)

    main()

    assert mock_consume_structured_logs.called == number_of_calls
    assert mock_consume_local_artifacts.called == 1 - number_of_calls


@pytest.mark.parametrize(
    "command_line, os_envs, function_kwargs",
    [
        (
            [
                "dbt-ol",
                CONSUME_STRUCTURED_LOGS_COMMAND_OPTION,
                "run",
                "--select",
                "orders",
                "--profiles-dir",
                "~/dbt/profiles-foo",
                "--project-dir",
                "~/dbt",
                "--target",
                "production",
                "--target-path",
                "dbt-target-1234",
                "--vars",
                '\'{"foo": "bar"}\'',
            ],
            {"OPENLINEAGE_NAMESPACE": "dbt"},
            {
                "args": [
                    "dbt",
                    "run",
                    "--select",
                    "orders",
                    "--profiles-dir",
                    "~/dbt/profiles-foo",
                    "--project-dir",
                    "~/dbt",
                    "--target",
                    "production",
                    "--target-path",
                    "dbt-target-1234",
                    "--vars",
                    '\'{"foo": "bar"}\'',
                ],
                "target": "production",
                "target_path": "dbt-target-1234",
                "project_dir": "~/dbt",
                "profile_name": None,
                "model_selector": None,
                "models": [],
            },
        )
    ],
    ids=["with_canonical_kwargs_for_consume_structured_logs"],
)
def test_consume_structured_logs(command_line, os_envs, function_kwargs, monkeypatch):
    mock_consume_structured_logs = mock.MagicMock()
    mockOpenLineageClient = mock.MagicMock()

    mock_dbt_structured_logs_processor = mock.MagicMock()
    mock_dbt_structured_logs_processor.parse = mock.MagicMock(return_value=[])
    mockDbtStructuredLogsProcessor = mock.MagicMock(
        return_value=mock_dbt_structured_logs_processor
    )  # instance returned
    mock_dbt_structured_logs_processor.dbt_command_return_code = 0

    monkeypatch.setattr("os.environ", os_envs)
    monkeypatch.setattr("openlineage.dbt.consume_structured_logs", mock_consume_structured_logs)
    monkeypatch.setattr("sys.argv", command_line)
    monkeypatch.setattr("openlineage.dbt.OpenLineageClient", mockOpenLineageClient)
    monkeypatch.setattr("openlineage.dbt.DbtStructuredLogsProcessor", mockDbtStructuredLogsProcessor)

    consume_structured_logs(**function_kwargs)

    expected_dbt_command = [
        "dbt",
        "run",
        "--select",
        "orders",
        "--profiles-dir",
        "~/dbt/profiles-foo",
        "--project-dir",
        "~/dbt",
        "--target",
        "production",
        "--target-path",
        "dbt-target-1234",
        "--vars",
        '\'{"foo": "bar"}\'',
    ]

    mockDbtStructuredLogsProcessor.assert_called_once_with(
        project_dir=function_kwargs["project_dir"],
        dbt_command_line=expected_dbt_command,
        producer=PRODUCER,
        target=function_kwargs["target"],
        target_path=function_kwargs["target_path"],
        job_namespace=os_envs["OPENLINEAGE_NAMESPACE"],
        profile_name=function_kwargs["profile_name"],
        logger=mock.ANY,
        models=function_kwargs["models"],
        selector=function_kwargs["model_selector"],
        openlineage_job_name=None,
    )


def _setup_local_artifacts_mocks(monkeypatch, env):
    """Wire up just enough mocks to drive consume_local_artifacts() up to the
    point where it sets ``processor.dbt_run_metadata``."""
    monkeypatch.setattr("os.environ", env)
    mock_processor = mock.MagicMock()
    mock_processor.parse.return_value.events.return_value = []
    mock_processor.run_result_path = "/nonexistent/run_results.json"
    mock_processor.job_name = "dbt-job-name"
    monkeypatch.setattr(
        "openlineage.dbt.DbtLocalArtifactProcessor", mock.MagicMock(return_value=mock_processor)
    )
    monkeypatch.setattr("openlineage.dbt.OpenLineageClient", mock.MagicMock())

    process_mock = mock.MagicMock()
    process_mock.__enter__ = mock.MagicMock(return_value=process_mock)
    process_mock.__exit__ = mock.MagicMock(return_value=None)
    process_mock.wait.return_value = 0
    monkeypatch.setattr("openlineage.dbt.subprocess.Popen", mock.MagicMock(return_value=process_mock))

    return mock_processor


def test_consume_local_artifacts_propagates_root_parent_metadata(monkeypatch):
    """Regression: child node events must keep the orchestrator's root parent so
    backends can stitch the full Airflow → dbt-run → node lineage chain."""
    from openlineage.dbt import consume_local_artifacts

    parent_namespace = "airflow"
    parent_job = "airflow-dag.task"
    parent_run_id = "f99310b4-3c3c-1a1a-2b2b-c1b95c24ff11"
    env = {
        "OPENLINEAGE_NAMESPACE": "dbt",
        "OPENLINEAGE_PARENT_ID": f"{parent_namespace}/{parent_job}/{parent_run_id}",
    }
    processor = _setup_local_artifacts_mocks(monkeypatch, env)

    consume_local_artifacts(
        args=["dbt", "run"],
        target=None,
        target_path=None,
        project_dir="./",
        profile_name=None,
        model_selector=None,
        models=[],
    )

    # Child events use processor.dbt_run_metadata as their parent facet — root must
    # point at the orchestrator that started us, not be left blank.
    md = processor.dbt_run_metadata
    assert md.root_parent_run_id == parent_run_id
    assert md.root_parent_job_name == parent_job
    assert md.root_parent_job_namespace == parent_namespace


def test_consume_local_artifacts_root_when_no_external_parent(monkeypatch):
    """When no orchestrator wraps dbt-ol, the dbt run is itself the root —
    so child events get root pointing at the dbt-run wrapper."""
    from openlineage.dbt import consume_local_artifacts

    env = {"OPENLINEAGE_NAMESPACE": "dbt"}  # no OPENLINEAGE_PARENT_ID
    processor = _setup_local_artifacts_mocks(monkeypatch, env)

    consume_local_artifacts(
        args=["dbt", "run"],
        target=None,
        target_path=None,
        project_dir="./",
        profile_name=None,
        model_selector=None,
        models=[],
    )

    md = processor.dbt_run_metadata
    # Root falls back to the wrapper's own start event.
    assert md.root_parent_run_id == md.run_id
    assert md.root_parent_job_name == md.job_name
    assert md.root_parent_job_namespace == md.job_namespace


def test_consume_local_artifacts_explicit_root_different_from_parent(monkeypatch):
    """When OPENLINEAGE_ROOT_PARENT_ID is provided and distinct from the immediate
    parent, child events must use root values — not parent values — for the root field."""
    from openlineage.dbt import consume_local_artifacts

    parent_namespace = "airflow"
    parent_job = "airflow-dag.task"
    parent_run_id = "aaaaaaaa-0000-0000-0000-000000000001"
    root_namespace = "prefect"
    root_job = "root-flow"
    root_run_id = "bbbbbbbb-0000-0000-0000-000000000002"
    env = {
        "OPENLINEAGE_NAMESPACE": "dbt",
        "OPENLINEAGE_PARENT_ID": f"{parent_namespace}/{parent_job}/{parent_run_id}",
        "OPENLINEAGE_ROOT_PARENT_ID": f"{root_namespace}/{root_job}/{root_run_id}",
    }
    processor = _setup_local_artifacts_mocks(monkeypatch, env)

    consume_local_artifacts(
        args=["dbt", "run"],
        target=None,
        target_path=None,
        project_dir="./",
        profile_name=None,
        model_selector=None,
        models=[],
    )

    md = processor.dbt_run_metadata
    # Root must reflect the explicit root, not the immediate parent.
    assert md.root_parent_run_id == root_run_id
    assert md.root_parent_job_name == root_job
    assert md.root_parent_job_namespace == root_namespace
    # Immediate parent stays as-is.
    assert md.run_id != root_run_id
    assert md.job_name != root_job


def test_consume_local_artifacts_partial_root_falls_back_to_parent(monkeypatch):
    """When OPENLINEAGE_ROOT_PARENT_ID is malformed (not all three identifiers
    present), root info must not be partially applied — it should fall back
    entirely to the immediate parent values."""
    from openlineage.dbt import consume_local_artifacts

    parent_namespace = "airflow"
    parent_job = "airflow-dag.task"
    parent_run_id = "cccccccc-0000-0000-0000-000000000003"
    env = {
        "OPENLINEAGE_NAMESPACE": "dbt",
        "OPENLINEAGE_PARENT_ID": f"{parent_namespace}/{parent_job}/{parent_run_id}",
        "OPENLINEAGE_ROOT_PARENT_ID": "invalid-not-three-parts",
    }
    processor = _setup_local_artifacts_mocks(monkeypatch, env)

    consume_local_artifacts(
        args=["dbt", "run"],
        target=None,
        target_path=None,
        project_dir="./",
        profile_name=None,
        model_selector=None,
        models=[],
    )

    md = processor.dbt_run_metadata
    # Partial root data must not bleed into result; fall back to the parent entirely.
    assert md.root_parent_run_id == parent_run_id
    assert md.root_parent_job_name == parent_job
    assert md.root_parent_job_namespace == parent_namespace
