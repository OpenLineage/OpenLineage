# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import attr


@attr.define
class FacetsConfig:
    environment_variables: list[str] = attr.field(factory=list)
