# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from openlineage.client.utils import deep_merge_dicts, import_from_string, try_import_from_string


def test_import_from_string():
    from openlineage.client.filter import ExactMatchFilter

    result = import_from_string("openlineage.client.filter.ExactMatchFilter")
    assert result == ExactMatchFilter


def test_import_from_string_unknown_path():
    with pytest.raises(ImportError):
        import_from_string("openlineage.client.non-existing-module")


def test_try_import_from_string():
    from openlineage.client.filter import ExactMatchFilter

    result = try_import_from_string("openlineage.client.filter.ExactMatchFilter")
    assert result == ExactMatchFilter


def test_try_import_from_string_unknown():
    result = try_import_from_string("openlineage.client.non-existing-module")
    assert result is None


def test_deep_merge_dicts_simple():
    dict1 = {"a": 1, "b": 2}
    dict2 = {"b": 3, "c": 4}
    expected = {"a": 1, "b": 3, "c": 4}
    assert deep_merge_dicts(dict1, dict2) == expected


def test_deep_merge_dicts_nested():
    dict1 = {"a": {"x": 1}, "b": 2}
    dict2 = {"a": {"y": 2}, "b": 3, "c": 4}
    expected = {"a": {"x": 1, "y": 2}, "b": 3, "c": 4}
    assert deep_merge_dicts(dict1, dict2) == expected


def test_deep_merge_dicts_overwrite():
    dict1 = {"a": {"x": 1}}
    dict2 = {"a": {"x": 2}}
    expected = {"a": {"x": 2}}
    assert deep_merge_dicts(dict1, dict2) == expected


def test_deep_merge_dicts_non_dict_values():
    dict1 = {"a": 1, "b": {"x": 1}}
    dict2 = {"b": 2, "c": 3}
    expected = {"a": 1, "b": 2, "c": 3}
    assert deep_merge_dicts(dict1, dict2) == expected


def test_deep_merge_dicts_empty_dicts():
    dict1 = {}
    dict2 = {"a": 1}
    expected = {"a": 1}
    assert deep_merge_dicts(dict1, dict2) == expected
