# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import Any

import attr

log = logging.getLogger(__name__)


def _to_string_tags(tags: dict[Any, Any] | None) -> dict[str, str]:
    """Coerce tag keys and values to strings, as required by the tags facets.

    Tags may come from sources that do not preserve string types, e.g. unquoted YAML scalars
    (`adhoc: true`) or JSON passed in the `OPENLINEAGE__TAGS` environment variable. Booleans are
    rendered the way they were written in those sources (`true`/`false`). Tags without a value
    are skipped, as a tag value is required by the spec.
    """
    result: dict[str, str] = {}
    for key, value in (tags or {}).items():
        if value is None:
            log.warning("OpenLineage tag `%s` has no value and will be ignored.", key)
            continue
        if isinstance(value, bool):
            value = "true" if value else "false"
        result[str(key)] = str(value)
    return result


@attr.define
class TagsConfig:
    job: dict[str, str] = attr.field(factory=dict, converter=_to_string_tags)
    run: dict[str, str] = attr.field(factory=dict, converter=_to_string_tags)
