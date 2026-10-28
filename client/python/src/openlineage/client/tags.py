# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import Any

import attr
from openlineage.client.utils import config_value_to_str

log = logging.getLogger(__name__)


def _tag_values_to_str(tags: dict[Any, Any] | None) -> dict[str, str]:
    """Normalize tag values to strings, as the Tags facets require.

    Environment variables are JSON-decoded and YAML scalars are typed, so `true`, `42` or `1.0`
    arrive as bool/int/float; they are turned back into "true", "42" and "1.0". A `null` value
    (or an empty YAML value) cannot form a valid tag, so the tag is skipped with a warning, as are
    objects and lists.
    """
    result: dict[str, str] = {}
    for key, value in (tags or {}).items():
        if value is None:
            log.warning(
                "Skipping OpenLineage tag `%s`: its value is null. To use the literal string "
                '`null`, quote it in YAML or set the environment variable to `"null"`.',
                key,
            )
            continue
        try:
            result[config_value_to_str(key)] = config_value_to_str(value)
        except TypeError:
            log.warning(
                "Skipping OpenLineage tag `%s`: its value must be a string, boolean or number, got `%r`.",
                key,
                value,
            )
    return result


@attr.define
class TagsConfig:
    job: dict[str, str] = attr.field(factory=dict, converter=_tag_values_to_str)
    run: dict[str, str] = attr.field(factory=dict, converter=_tag_values_to_str)
