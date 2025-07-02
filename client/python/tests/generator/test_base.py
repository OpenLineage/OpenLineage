# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import pathlib
import tempfile
from unittest import mock

import pytest
from openlineage.client.generator.base import (
    BASE_IDS,
    SCHEMA_URLS,
    camel_to_snake,
    deep_merge_dicts,
    load_specs,
    parse_additional_data,
    separate_imports,
)


def test_camel_to_snake() -> None:
    assert camel_to_snake("camelCase") == "camel_case"
    assert camel_to_snake("CamelCase") == "camel_case"
    assert camel_to_snake("HTTPResponse") == "http_response"
    assert camel_to_snake("already_snake") == "already_snake"
    assert camel_to_snake("DatasetFacet") == "dataset_facet"


def test_deep_merge_dicts() -> None:
    # Test basic merge
    dict1 = {"a": 1, "b": 2}
    dict2 = {"b": 3, "c": 4}
    result = deep_merge_dicts(dict1, dict2)
    assert result == {"a": 1, "b": 3, "c": 4}

    # Test nested dictionaries
    dict1 = {"a": 1, "nested": {"x": 1, "y": 2}}
    dict2 = {"b": 3, "nested": {"y": 3, "z": 4}}
    result = deep_merge_dicts(dict1, dict2)
    assert result == {"a": 1, "b": 3, "nested": {"x": 1, "y": 3, "z": 4}}


def test_parse_additional_data() -> None:
    # Reset global dictionaries
    SCHEMA_URLS.clear()
    BASE_IDS.clear()

    spec = {
        "$id": "https://openlineage.io/spec/facets/1-0-0/TestFacet.json",
        "$defs": {"TestFacet": {"type": "object"}, "AnotherFacet": {"type": "object"}},
    }

    parse_additional_data(spec, "TestFacet.json")

    assert (
        SCHEMA_URLS["TestFacet"] == "https://openlineage.io/spec/facets/1-0-0/TestFacet.json#/$defs/TestFacet"
    )
    assert (
        SCHEMA_URLS["AnotherFacet"]
        == "https://openlineage.io/spec/facets/1-0-0/TestFacet.json#/$defs/AnotherFacet"
    )
    assert BASE_IDS["TestFacet.json"] == "https://openlineage.io/spec/facets/1-0-0/TestFacet.json"


@pytest.fixture
def sample_specs():
    with tempfile.TemporaryDirectory() as tmp_dir, tempfile.TemporaryDirectory() as facet_tmp_dir:
        base_spec = pathlib.Path(tmp_dir) / "OpenLineage.json"
        facet_spec = pathlib.Path(facet_tmp_dir) / "TestFacet.json"

        base_spec_content = {
            "$id": "https://openlineage.io/spec/1-0-0/OpenLineage.json",
            "$defs": {"RunFacet": {"type": "object"}},
        }

        facet_spec_content = {
            "$id": "https://openlineage.io/spec/facets/1-0-0/TestFacet.json",
            "$defs": {"TestFacet": {"type": "object"}},
        }

        base_spec.write_text(json.dumps(base_spec_content))
        facet_spec.write_text(json.dumps(facet_spec_content))

        yield base_spec, facet_spec


def test_load_specs(sample_specs) -> None:
    base_spec, facet_spec = sample_specs
    facet_dir = facet_spec.parent

    # Test with directory of facets
    locations = load_specs(base_spec, facet_dir)
    assert len(locations) == 2  # noqa: PLR2004
    assert locations[0] == base_spec.resolve()
    assert locations[1] == facet_spec

    # Test with single facet file
    locations = load_specs(base_spec, facet_spec)
    assert len(locations) == 2  # noqa: PLR2004
    assert locations[0] == base_spec.resolve()
    assert locations[1] == facet_spec

    assert "OpenLineage.json" in BASE_IDS
    assert "TestFacet.json" in BASE_IDS
    assert "RunFacet" in SCHEMA_URLS
    assert "TestFacet" in SCHEMA_URLS


def test_separate_imports() -> None:
    code = """import os
import sys
from typing import Dict, List

class MyClass:
    def __init__(self):
        pass

def my_function():
    return True
"""
    imports, rest = separate_imports(code)
    assert imports == "import os\nimport sys\nfrom typing import Dict, List"
    assert "class MyClass:" in rest
    assert "def my_function():" in rest


@mock.patch("openlineage.client.generator.base.tempfile.NamedTemporaryFile")
@mock.patch("openlineage.client.generator.base.subprocess.Popen")
@mock.patch("openlineage.client.generator.base.os.rename")
def test_format_and_save_output(mock_rename, mock_popen, mock_tempfile, tmp_path) -> None:
    from openlineage.client.generator.base import format_and_save_output

    mock_file = mock.MagicMock()
    mock_tempfile.return_value.__enter__.return_value = mock_file

    mock_process = mock.MagicMock()
    mock_process.returncode = 0
    mock_popen.return_value.__enter__.return_value = mock_process

    output_location = tmp_path / "test_output.py"

    format_and_save_output("from .OpenLineage import RunEvent", output_location)

    mock_file.write.assert_any_call("from openlineage.client.generated.base import RunEvent")

    # Check rename was called
    mock_rename.assert_called_once()
