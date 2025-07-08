# Copyright 2018-2025 contributors to the OpenLineage project
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
                "target": "production",
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
        job_namespace=os_envs["OPENLINEAGE_NAMESPACE"],
        profile_name=function_kwargs["profile_name"],
        logger=mock.ANY,
        models=function_kwargs["models"],
        selector=function_kwargs["model_selector"],
    )
