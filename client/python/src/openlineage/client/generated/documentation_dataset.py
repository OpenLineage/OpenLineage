# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import attr
from openlineage.client.generated.base import DatasetFacet


@attr.define
class DocumentationDatasetFacet(DatasetFacet):
    description: str
    """The description of the dataset."""

    contentType: str | None = attr.field(default=None)  # noqa: N815
    """MIME type of the description field content."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-1-0/DocumentationDatasetFacet.json#/$defs/DocumentationDatasetFacet"
