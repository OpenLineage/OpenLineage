# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from enum import Enum

import attr
from openlineage.client.generated.base import DatasetFacet
from openlineage.client.utils import RedactMixin


class LifecycleStateChange(Enum):
    """The lifecycle state change."""

    ALTER = "ALTER"
    CREATE = "CREATE"
    DROP = "DROP"
    OVERWRITE = "OVERWRITE"
    RENAME = "RENAME"
    TRUNCATE = "TRUNCATE"


@attr.define
class LifecycleStateChangeDatasetFacet(DatasetFacet):
    lifecycleStateChange: LifecycleStateChange  # noqa: N815
    """The lifecycle state change."""

    previousIdentifier: PreviousIdentifier | None = attr.field(default=None)  # noqa: N815
    """Previous name of the dataset in case of renaming it."""

    @staticmethod
    def _get_schema() -> str:
        return "https://openlineage.io/spec/facets/1-0-1/LifecycleStateChangeDatasetFacet.json#/$defs/LifecycleStateChangeDatasetFacet"


@attr.define
class PreviousIdentifier(RedactMixin):
    """Previous name of the dataset in case of renaming it."""

    name: str
    namespace: str
