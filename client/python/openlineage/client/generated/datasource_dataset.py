# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations
from typing import ClassVar, List, Optional
from attr import define, field
from openlineage.client import utils
from openlineage.client.generated.base import DatasetFacet


@define
class DatasourceDatasetFacet(DatasetFacet):
    name: Optional[str] = field(default=None)
    uri: Optional[str] = field(default=None)
    _additional_skip_redact: ClassVar[List[str]] = ["name", "uri"]

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet"

    @uri.validator
    def uri_check(self, attribute: str, value: str) -> None:  # noqa: ARG002
        from urllib.parse import urlparse

        urlparse(value)


utils.register_facet_key("dataSource", DatasourceDatasetFacet)
