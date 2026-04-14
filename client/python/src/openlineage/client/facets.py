# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import attr


@attr.define
class SourceCodeLocationConfig:
    disabled: bool = True
    repo_url: str | None = None
    version: str | None = None
    tag: str | None = None
    branch: str | None = None
    pull_request_number: str | None = None


@attr.define
class FacetsConfig:
    environment_variables: list[str] = attr.field(factory=list)
    source_code_location: SourceCodeLocationConfig = attr.field(factory=SourceCodeLocationConfig)
