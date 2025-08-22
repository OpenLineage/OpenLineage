# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pathlib
import tempfile
from unittest import mock

from click.testing import CliRunner
from openlineage.client.generator.cli import get_base_spec, main


@mock.patch("openlineage.client.generator.cli.httpx.get")
def test_get_base_spec(mock_get) -> None:
    mock_response = mock.MagicMock()
    mock_response.text = '{"$id": "test_id", "$defs": {}}'
    mock_get.return_value = mock_response

    result = get_base_spec()

    mock_get.assert_called_once_with("https://openlineage.io/spec/2-0-2/OpenLineage.json", timeout=10)

    assert result.exists()
    assert result.read_text() == '{"$id": "test_id", "$defs": {}}'


@mock.patch("openlineage.client.generator.cli.get_base_spec")
@mock.patch("openlineage.client.generator.cli.load_specs")
@mock.patch("openlineage.client.generator.cli.parse_and_generate")
def test_cli_main_with_output_location(mock_parse_generate, mock_load_specs, mock_get_base_spec) -> None:
    mock_base_spec = pathlib.Path("/mock/OpenLineage.json")
    mock_get_base_spec.return_value = mock_base_spec

    mock_load_specs.return_value = [mock_base_spec]

    class MockResult:
        def __init__(self, body):
            self.body = body

    mock_parse_generate.return_value = {("TestFacet",): MockResult("test facet content")}

    runner = CliRunner()

    with tempfile.TemporaryDirectory() as tmp_dir:
        facets_spec_location = pathlib.Path(tmp_dir) / "facets"
        facets_spec_location.mkdir()

        output_location = pathlib.Path(tmp_dir) / "output"

        result = runner.invoke(main, [str(facets_spec_location), "--output-location", str(output_location)])

        assert result.exit_code == 0
        assert mock_parse_generate.called
