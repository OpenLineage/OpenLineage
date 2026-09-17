# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Map collected Datasphere metadata to an OpenLineage DatasetEvent.

Design rule: prefer **standard** OpenLineage facets over custom ones. Everything Datasphere's OData
catalog exposes is carried in a standard facet where one exists:

  * schema (columns + business labels)      -> ``schema``          (SchemaDatasetFacet)
  * rowCount / lastUpdated                   -> ``dataQualityMetrics`` (DataQualityMetricsDatasetFacet)
  * business label (human name)              -> ``documentation``    (DocumentationDatasetFacet)
  * technical name, space, catalog URLs,     -> ``catalog``          (CatalogDatasetFacet)
    supportsAnalyticalQueries, hasParameters      (SAP-specific bits land in ``catalogProperties``)
  * source tenant / base URL                 -> ``dataSource``       (DatasourceDatasetFacet)
  * OData consumption data URL(s)            -> ``symlinks``         (SymlinksDatasetFacet) -- the
                                                concrete, resolvable location of the same dataset.
  * tags                                     -> ``tags``  (TagsDatasetFacet) -- when a tag source
                                                exists (not in the OData catalog today; see README).

The only custom facet is ``odataError`` -- OpenLineage has no standard equivalent for "this
metadata-scan hit a non-2xx OData response".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlsplit

from openlineage.client.event_v2 import DatasetEvent, StaticDataset
from openlineage.client.facet_v2 import catalog_dataset as catalog
from openlineage.client.facet_v2 import data_quality_metrics_dataset as dq
from openlineage.client.facet_v2 import datasource_dataset as datasource
from openlineage.client.facet_v2 import documentation_dataset as documentation
from openlineage.client.facet_v2 import schema_dataset
from openlineage.client.facet_v2 import symlinks_dataset as symlinks

from app.datasphere.errors import ODataError
from app.datasphere.models import Dataset, DatasetSchema
from app.lastupdated.base import LastUpdated
from app.lineage.facets import odata_error_facet


@dataclass
class DatasetMetadata:
    """Everything collected for one dataset during a scan (any field may be missing on error)."""

    dataset: Dataset
    schema: DatasetSchema | None = None
    row_count: int | None = None
    last_updated: LastUpdated | None = None
    space_label: str | None = None  # human label of the containing space, if known
    errors: list[ODataError] = field(default_factory=list)


def dataset_namespace(tenant_host: str) -> str:
    return f"sap:datasphere://{tenant_host}"


def dataset_name(dataset: Dataset) -> str:
    return f"{dataset.space}.{dataset.asset_id}"


def _schema_facet(schema: DatasetSchema) -> schema_dataset.SchemaDatasetFacet:
    return schema_dataset.SchemaDatasetFacet(
        fields=[
            schema_dataset.SchemaDatasetFacetFields(
                name=col.name,
                type=col.type,
                description=col.description,
            )
            for col in schema.columns
        ]
    )


def _data_quality_facet(
    row_count: int | None, last_updated: LastUpdated | None
) -> dq.DataQualityMetricsDatasetFacet | None:
    if row_count is None and (last_updated is None or not last_updated.found):
        return None
    last_updated_iso = None
    if last_updated and last_updated.value is not None:
        last_updated_iso = last_updated.value.isoformat()
    facet = dq.DataQualityMetricsDatasetFacet(
        columnMetrics={},  # required by the spec; no per-column stats collected
        rowCount=row_count,
        lastUpdated=last_updated_iso,
    )
    # Record where lastUpdated came from (e.g. "design_time:deployment_date") for transparency.
    if last_updated and last_updated.source:
        facet = facet.with_additional_properties(lastUpdatedSource=last_updated.source)
    return facet


def _documentation_facet(dataset: Dataset) -> documentation.DocumentationDatasetFacet | None:
    # ``name`` is the business label (falls back to asset_id). Emit the human name as the description
    # only when it actually differs from the technical name.
    if dataset.name and dataset.name != dataset.asset_id:
        return documentation.DocumentationDatasetFacet(description=dataset.name)
    return None


def _catalog_facet(dataset: Dataset, space_label: str | None) -> catalog.CatalogDatasetFacet:
    # Free-form catalogProperties (string-valued) carry the SAP-specific bits that have no dedicated
    # standard field. Only include populated values.
    # Access model: which OData consumption endpoints the asset exposes.
    if dataset.relational_data_url and dataset.analytical_data_url:
        access_model = "relational+analytical"
    elif dataset.analytical_data_url:
        access_model = "analytical"
    else:
        access_model = "relational"

    props: dict[str, str] = {
        "businessName": dataset.name,
        "space": dataset.space,
        "entitySet": dataset.entity_set,
        "accessModel": access_model,
        "supportsAnalyticalQueries": str(dataset.supports_analytical).lower(),
        "hasParameters": str(dataset.has_parameters).lower(),
    }
    if space_label:
        props["spaceLabel"] = space_label
    if dataset.analytical_metadata_url:
        props["analyticalMetadataUrl"] = dataset.analytical_metadata_url
    if dataset.analytical_data_url:
        props["analyticalDataUrl"] = dataset.analytical_data_url

    return catalog.CatalogDatasetFacet(
        # framework = the catalog; type = how it is consumed. The asset's relational/analytical access
        # model is a property of the asset (catalogProperties.accessModel), not the catalog type.
        framework="sap-datasphere",
        type="odata",
        name=dataset.asset_id,  # technical name
        metadataUri=dataset.relational_metadata_url or dataset.analytical_metadata_url,
        warehouseUri=dataset.relational_data_url or dataset.analytical_data_url,
        source=dataset.space,
        catalogProperties=props,
    )


def _datasource_facet(tenant_host: str, base_url: str | None) -> datasource.DatasourceDatasetFacet:
    return datasource.DatasourceDatasetFacet(
        name=tenant_host,
        uri=base_url or dataset_namespace(tenant_host),
    )


def _symlinks_facet(dataset: Dataset) -> symlinks.SymlinksDatasetFacet | None:
    """Alternate, resolvable coordinates for the same dataset.

    The OpenLineage dataset name (``<space>.<asset>`` under ``sap:datasphere://<tenant>``) is the
    stable logical identifier. The OData consumption data URL is the concrete, fetchable location of
    the same dataset, so expose it via the standard ``symlinks`` facet: each identifier splits the
    URL into ``namespace`` (``scheme://host``) and ``name`` (the path). Consumers can resolve a
    dataset to its exact OData endpoint without parsing the logical name.
    """
    identifiers: list[symlinks.Identifier] = []
    for url in (dataset.relational_data_url, dataset.analytical_data_url):
        if not url:
            continue
        parts = urlsplit(url)
        if not (parts.scheme and parts.netloc):
            continue
        identifiers.append(
            symlinks.Identifier(
                namespace=f"{parts.scheme}://{parts.netloc}",
                name=parts.path.strip("/"),
                type="TABLE",
            )
        )
    if not identifiers:
        return None
    return symlinks.SymlinksDatasetFacet(identifiers=identifiers)


def build_dataset_event(
    meta: DatasetMetadata,
    *,
    tenant_host: str,
    producer: str,
    base_url: str | None = None,
    event_time: datetime | None = None,
) -> DatasetEvent:
    facets: dict = {}
    if meta.schema and meta.schema.columns:
        facets["schema"] = _schema_facet(meta.schema)
    dq_facet = _data_quality_facet(meta.row_count, meta.last_updated)
    if dq_facet is not None:
        facets["dataQualityMetrics"] = dq_facet
    doc_facet = _documentation_facet(meta.dataset)
    if doc_facet is not None:
        facets["documentation"] = doc_facet
    facets["catalog"] = _catalog_facet(meta.dataset, meta.space_label)
    facets["dataSource"] = _datasource_facet(tenant_host, base_url)
    symlinks_facet = _symlinks_facet(meta.dataset)
    if symlinks_facet is not None:
        facets["symlinks"] = symlinks_facet
    if meta.errors:
        facets["odataError"] = odata_error_facet(meta.errors)

    event_time = event_time or datetime.now(timezone.utc)
    return DatasetEvent(
        eventTime=event_time.isoformat(),
        producer=producer,
        dataset=StaticDataset(
            namespace=dataset_namespace(tenant_host),
            name=dataset_name(meta.dataset),
            facets=facets,
        ),
    )
