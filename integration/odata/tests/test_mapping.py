# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone

from openlineage.client.serde import Serde

from app.datasphere.errors import ODataError
from app.datasphere.models import Column, Dataset, DatasetSchema
from app.lastupdated.base import LastUpdated
from app.lineage.mapping import (
    DatasetMetadata,
    build_dataset_event,
    dataset_name,
    dataset_namespace,
)

DS = Dataset(space="SAP_S4H", asset_id="sap.s4h.IL_A_CostCenter", name="CostCenter")
_LABEL = "Controlling Area"


def _facets(meta):
    ev = build_dataset_event(meta, tenant_host="host.example", producer="p")
    return Serde.to_dict(ev)["dataset"]["facets"], ev


def test_namespace_and_name():
    assert dataset_namespace("host.example") == "sap:datasphere://host.example"
    assert dataset_name(DS) == "SAP_S4H.sap.s4h.IL_A_CostCenter"


def test_full_event_has_all_facets():
    meta = DatasetMetadata(
        dataset=DS,
        schema=DatasetSchema(columns=[Column("ControllingArea", "Edm.String", False, True, _LABEL)]),
        row_count=1454,
        last_updated=LastUpdated(datetime(2025, 8, 29, tzinfo=timezone.utc), "design_time:deployment_date"),
        space_label="SAP S/4HANA Content (BDC)",
        errors=[ODataError("row_count", 500, "boom", "http://x", "c1")],
    )
    facets, ev = _facets(meta)
    assert facets["schema"]["fields"][0]["name"] == "ControllingArea"
    assert facets["dataQualityMetrics"]["rowCount"] == 1454
    assert facets["dataQualityMetrics"]["lastUpdated"].startswith("2025-08-29")
    assert facets["dataQualityMetrics"]["lastUpdatedSource"] == "design_time:deployment_date"
    assert facets["odataError"]["errors"][0]["httpStatus"] == 500
    assert ev.dataset.name == "SAP_S4H.sap.s4h.IL_A_CostCenter"
    # Business name -> standard documentation facet.
    assert facets["documentation"]["description"] == "CostCenter"
    # Technical name + SAP-specific bits -> standard catalog facet.
    assert facets["catalog"]["framework"] == "sap-datasphere"
    assert facets["catalog"]["name"] == "sap.s4h.IL_A_CostCenter"
    assert facets["catalog"]["source"] == "SAP_S4H"
    assert facets["catalog"]["catalogProperties"]["businessName"] == "CostCenter"
    assert facets["catalog"]["catalogProperties"]["spaceLabel"] == "SAP S/4HANA Content (BDC)"
    # Source tenant -> standard dataSource facet.
    assert facets["dataSource"]["name"] == "host.example"


def test_dq_facet_omitted_when_no_data():
    meta = DatasetMetadata(dataset=DS, schema=None, row_count=None, last_updated=None, errors=[])
    facets, _ = _facets(meta)
    assert "dataQualityMetrics" not in facets
    assert "schema" not in facets
    assert "odataError" not in facets
    # catalog + dataSource are always present (identity facets, no upstream data required).
    assert facets["catalog"]["name"] == "sap.s4h.IL_A_CostCenter"
    assert facets["dataSource"]["name"] == "host.example"
    # No consumption URLs on this fixture -> no symlinks facet.
    assert "symlinks" not in facets


def test_symlinks_facet_carries_odata_data_urls():
    base = "https://host.example/dwaas-core/odata/v4/consumption"
    ds = Dataset(
        space="SAP_S4H",
        asset_id="SAP.CURRENCY.VIEW.TCURR",
        name="Exchange Rate",
        relational_data_url=f"{base}/relational/SAP_S4H/SAP.CURRENCY.VIEW.TCURR/",
        analytical_data_url=f"{base}/analytical/SAP_S4H/SAP.CURRENCY.VIEW.TCURR/",
    )
    facets, _ = _facets(DatasetMetadata(dataset=ds))
    ids = facets["symlinks"]["identifiers"]
    assert len(ids) == 2  # relational + analytical
    # Each identifier splits the OData URL into scheme://host namespace + path name.
    assert all(i["namespace"] == "https://host.example" for i in ids)
    assert all(i["type"] == "TABLE" for i in ids)
    names = {i["name"] for i in ids}
    assert "dwaas-core/odata/v4/consumption/relational/SAP_S4H/SAP.CURRENCY.VIEW.TCURR" in names
    assert "dwaas-core/odata/v4/consumption/analytical/SAP_S4H/SAP.CURRENCY.VIEW.TCURR" in names


def test_row_count_only_produces_dq_facet():
    meta = DatasetMetadata(dataset=DS, row_count=0)
    facets, _ = _facets(meta)
    assert facets["dataQualityMetrics"]["rowCount"] == 0
    # None fields are omitted by the OpenLineage serializer.
    assert "lastUpdated" not in facets["dataQualityMetrics"]
