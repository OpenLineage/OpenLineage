# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import ClassVar

from attr import define, field
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


@define
class ColumnLineageDatasetFacet(DatasetFacet):
    fields: dict[str, Fields]
    """
    Column level lineage that maps output fields into input fields used to evaluate them.
    """

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-2/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet"


@define
class Fields(RedactMixin):
    inputFields: list[InputField]  # noqa: N815
    transformationDescription: str | None = field(default=None)  # noqa: N815
    """
    a string representation of the transformation applied
    """
    transformationType: str | None = field(default=None)  # noqa: N815
    """
    IDENTITY|MASKED reflects a clearly defined behavior. IDENTITY: exact same as input; MASKED: no
    original data available (like a hash of PII for example)
    """


@define
class InputField(RedactMixin):
    namespace: str
    """
    The input dataset namespace
    """
    name: str
    """
    The input dataset name
    """
    field: str
    """
    The input field
    """
    _skip_redact: ClassVar[list[str]] = ["namespace", "name", "field"]
