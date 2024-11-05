# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import attr


@attr.s
class FacetsConfig:
    environment_variables: list[str] = attr.ib(factory=list)
