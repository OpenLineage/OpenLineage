# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pickle

import pytest
from openlineage.client.facet_v2 import BaseFacet, JobFacet, documentation_job
from openlineage.client.serde import Serde
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


def test_deep_merge_dicts_scalar_replaced_by_dict():
    # dict2's value takes precedence when the two sides are not both dicts;
    # a scalar in dict1 must not be recursed into (it raised AttributeError).
    dict1 = {"transport": "http"}
    dict2 = {"transport": {"type": "console"}}
    expected = {"transport": {"type": "console"}}
    assert deep_merge_dicts(dict1, dict2) == expected


def test_deep_merge_dicts_empty_dicts():
    dict1 = {}
    dict2 = {"a": 1}
    expected = {"a": 1}
    assert deep_merge_dicts(dict1, dict2) == expected


def test_add_additional_properties_updates_aliased_private_fields():
    facet = documentation_job.DocumentationJobFacet(description="desc")
    updated = facet.with_additional_properties(
        producer="https://example.com/custom-producer",
        deleted=True,
        extra="value",
    )

    assert updated._producer == "https://example.com/custom-producer"  # noqa: SLF001
    assert updated._deleted is True  # noqa: SLF001
    assert updated.extra == "value"
    assert isinstance(updated, documentation_job.DocumentationJobFacet)
    assert isinstance(updated, JobFacet)

    pickled = pickle.loads(pickle.dumps(updated))  # noqa: S301
    assert pickled == updated
    assert pickled._producer == "https://example.com/custom-producer"  # noqa: SLF001
    assert pickled._deleted is True  # noqa: SLF001
    assert pickled.extra == "value"


def test_add_additional_properties_supports_leading_underscore_keys():
    facet = BaseFacet()
    updated = facet.with_additional_properties(_custom="val", normal="ok")

    assert updated._custom == "val"  # noqa: SLF001
    assert updated.normal == "ok"
    serialized = Serde.to_dict(updated)
    assert serialized["_custom"] == "val"
    assert serialized["normal"] == "ok"

    pickled = pickle.loads(pickle.dumps(updated))  # noqa: S301
    assert pickled == updated
    assert pickled._custom == "val"  # noqa: SLF001
    assert pickled.normal == "ok"


def test_add_additional_properties_reuses_class_when_no_new_fields():
    facet = documentation_job.DocumentationJobFacet(description="desc")
    updated = facet.with_additional_properties(
        description="new_desc",
        producer="https://example.com/custom-producer",
    )
    assert type(updated) is documentation_job.DocumentationJobFacet
    assert updated.description == "new_desc"
    assert updated._producer == "https://example.com/custom-producer"  # noqa: SLF001

    extended = facet.with_additional_properties(custom="v1")
    extended_updated = extended.with_additional_properties(custom="v2")
    assert type(extended_updated) is type(extended)
    assert extended_updated.custom == "v2"
    assert pickle.loads(pickle.dumps(extended_updated)) == extended_updated  # noqa: S301
