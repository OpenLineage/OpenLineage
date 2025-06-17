# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING

import attr

if TYPE_CHECKING:
    from openlineage.client.generated.tags_job import TagsJobFacetFields
    from openlineage.client.generated.tags_run import TagsRunFacetFields


@attr.define
class TagsConfig:
    job: list[TagsJobFacetFields] = attr.field(factory=list)
    run: list[TagsRunFacetFields] = attr.field(factory=list)
