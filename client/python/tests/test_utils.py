# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections import defaultdict
from unittest.mock import mock_open, patch

from openlineage.client.utils import load_config


@patch("openlineage.client.utils._find_yaml", return_value="ol.yml")
@patch("builtins.open", mock_open(read_data=""))
@patch("yaml.safe_load", return_value={"database": True})
def test_load_config_with_values(mock_safe_load, mock_find_yaml):  # noqa: ARG001
    config = load_config()
    assert config == {"database": True}


@patch("openlineage.client.utils._find_yaml", return_value="empty_config.yaml")
@patch("builtins.open", mock_open(read_data=""))
@patch("yaml.safe_load", return_value=None)
def test_load_config_empty_file(mock_safe_load, mock_find_yaml):  # noqa: ARG001
    config = load_config()
    assert config == defaultdict(dict)


@patch("openlineage.client.utils._find_yaml", return_value="empty_config.yaml")
@patch("builtins.open", mock_open(read_data=""))
@patch("yaml.safe_load", return_value={})
def test_load_config_empty_dict(mock_safe_load, mock_find_yaml):  # noqa: ARG001
    config = load_config()
    assert config == defaultdict(dict)


@patch("openlineage.client.utils._find_yaml", return_value=None)
def test_load_config_file_not_found(mock_find_yaml):  # noqa: ARG001
    config = load_config()
    assert config == defaultdict(dict)
