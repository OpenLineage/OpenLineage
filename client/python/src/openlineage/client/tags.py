# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import attr


@attr.define
class TagsConfig:
    job: dict[str, str] = attr.field(factory=dict)
    run: dict[str, str] = attr.field(factory=dict)
