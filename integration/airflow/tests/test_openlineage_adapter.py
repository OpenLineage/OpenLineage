# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from unittest import mock
from unittest.mock import MagicMock, patch

from openlineage.airflow.adapter import OpenLineageAdapter


@patch.dict(
    os.environ, {"MARQUEZ_URL": "http://marquez:5000", "MARQUEZ_API_KEY": "api-key"}
)
def test_create_client_from_marquez_url():
    client = OpenLineageAdapter().get_or_create_openlineage_client()
    assert client.transport.url == "http://marquez:5000"


@patch.dict(
    os.environ,
    {"OPENLINEAGE_URL": "http://ol-api:5000", "OPENLINEAGE_API_KEY": "api-key"},
)
def test_create_client_from_ol_env():
    client = OpenLineageAdapter().get_or_create_openlineage_client()
    assert client.transport.url == "http://ol-api:5000"


def test_setting_ol_adapter_log_level() -> None:
    # DEBUG level set for `openlineage` logger in tests setup
    default_log_level = logging.DEBUG
    # without environment variable
    OpenLineageAdapter()
    parent_logger = logging.getLogger("openlineage.airflow")
    logger = logging.getLogger("openlineage.airflow.adapter")
    assert parent_logger.getEffectiveLevel() == default_log_level
    assert logger.getEffectiveLevel() == default_log_level
    with patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_LOGGING": "CRITICAL"}):
        assert parent_logger.getEffectiveLevel() == default_log_level
        assert logger.getEffectiveLevel() == default_log_level
        OpenLineageAdapter()
        assert parent_logger.getEffectiveLevel() == logging.CRITICAL
        assert logger.getEffectiveLevel() == logging.CRITICAL


@patch(
    "openlineage.airflow.adapter.OpenLineageAdapter.get_or_create_openlineage_client"
)
@patch("openlineage.airflow.adapter.redact_with_exclusions")
@patch("openlineage.airflow.adapter.Stats.incr")
@patch("openlineage.airflow.adapter.Stats.timer")
def test_openlineage_adapter_stats_emit_success(
    mock_stats_timer, mock_stats_incr, mock_redact, mock_get_client
):
    adapter = OpenLineageAdapter()

    adapter.emit(MagicMock())

    mock_stats_incr.assert_not_called()
    mock_stats_timer.assert_called_with("ol.emit.attempts")


@patch(
    "openlineage.airflow.adapter.OpenLineageAdapter.get_or_create_openlineage_client"
)
@patch("openlineage.airflow.adapter.redact_with_exclusions")
@patch("openlineage.airflow.adapter.Stats.incr")
@patch("openlineage.airflow.adapter.Stats.timer")
def test_openlineage_adapter_stats_emit_failed(
    mock_stats_timer, mock_stats_incr, mock_redact, mock_get_client
):
    adapter = OpenLineageAdapter()
    mock_get_client.return_value.emit.side_effect = Exception()

    adapter.emit(MagicMock())

    mock_stats_timer.assert_called_with("ol.emit.attempts")
    mock_stats_incr.assert_has_calls([mock.call("ol.emit.failed")])
