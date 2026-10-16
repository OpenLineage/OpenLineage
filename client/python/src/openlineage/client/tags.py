# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import attr


def _coerce_str_dict(value: dict) -> dict[str, str]:
    """Coerce dict values to str, dropping None values.

    Tags facet schema requires `value` to be a string.  JSON-decoded env vars
    and unquoted YAML scalars can introduce booleans, numbers, or None; this
    converter normalises them so the emitted event stays schema-valid.
    """
    return {k: str(v) for k, v in value.items() if v is not None}


@attr.define
class TagsConfig:
    job: dict[str, str] = attr.field(factory=dict, converter=_coerce_str_dict)
    run: dict[str, str] = attr.field(factory=dict, converter=_coerce_str_dict)
