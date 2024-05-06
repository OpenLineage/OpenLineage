# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from openlineage.client.utils import import_from_string, try_import_from_string


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
