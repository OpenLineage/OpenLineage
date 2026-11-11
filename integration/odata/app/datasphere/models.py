# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Backend-neutral domain models produced by a DatasphereClient."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class Column:
    name: str
    type: str  # OData/EDMX type as reported by the backend (e.g. "Edm.String")
    nullable: bool = True
    is_key: bool = False
    description: str | None = None  # business label, if available


@dataclass(frozen=True)
class Dataset:
    """A consumable Datasphere asset (exposed for consumption) within a space.

    ``asset_id`` is the technical name (e.g. ``SAP.TIME.VIEW_DIMENSION_YEAR``); ``entity_set`` is the
    OData entity-set id (asset id with ``.`` -> ``_``) and is derived automatically when not given.
    The catalog supplies absolute OData URLs for the asset's metadata and data.
    """

    space: str
    asset_id: str
    name: str  # human-facing label (falls back to asset_id)
    kind: str = "entity"
    entity_set: str = ""
    relational_metadata_url: str | None = None
    relational_data_url: str | None = None
    analytical_metadata_url: str | None = None
    analytical_data_url: str | None = None
    supports_analytical: bool = False
    has_parameters: bool = False
    # Design-time timestamps (not available via the OData catalog; kept for other backends/tests).
    deployment_date: datetime | None = None
    modification_date: datetime | None = None

    def __post_init__(self) -> None:
        if not self.entity_set:
            object.__setattr__(self, "entity_set", self.asset_id.replace(".", "_"))


@dataclass(frozen=True)
class Space:
    id: str
    name: str = ""


@dataclass
class DatasetSchema:
    columns: list[Column] = field(default_factory=list)

    @property
    def key_columns(self) -> list[Column]:
        return [c for c in self.columns if c.is_key]
