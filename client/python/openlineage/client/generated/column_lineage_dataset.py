# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations
from openlineage.client.utils import RedactMixin
from attr import define, field
from openlineage.client import utils
from typing import ClassVar, Dict, List, Optional
from openlineage.client.generated.base import DatasetFacet


@define
class InputField(RedactMixin):
    namespace: str
    """The input dataset namespace"""

    name: str
    """The input dataset name"""

    field: str
    """The input field"""

    _skip_redact: ClassVar[List[str]] = ["namespace", "name", "field"]


@define
class Fields(RedactMixin):
    inputFields: List[InputField]  # noqa: N815
    transformationDescription: Optional[str] = field(default=None)  # noqa: N815
    """a string representation of the transformation applied"""

    transformationType: Optional[str] = field(default=None)  # noqa: N815
    """
    IDENTITY|MASKED reflects a clearly defined behavior. IDENTITY: exact same as input; MASKED: no
    original data available (like a hash of PII for example)
    """


@define
class ColumnLineageDatasetFacet(DatasetFacet):
    fields: Dict[str, Fields]
    """Column level lineage that maps output fields into input fields used to evaluate them."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-2/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet"


utils.register_facet_key("columnLineage", ColumnLineageDatasetFacet)
