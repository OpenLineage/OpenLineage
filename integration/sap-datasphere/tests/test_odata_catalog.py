# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from app.datasphere.models import Dataset
from app.datasphere.odata_client import parse_catalog_assets, parse_catalog_spaces

SPACES = [
    {"name": "SAP_S4H", "label": "SAP S/4HANA Content (BDC)"},
    {"name": "USR0569094", "label": "USR0569094"},
    {"label": "no-name-skipped"},
]

ASSETS = [
    {
        "name": "SAP.TIME.VIEW_DIMENSION_YEAR",
        "label": "Time Dimension - Year",
        "spaceName": "SAP_S4H",
        "assetRelationalMetadataUrl": "https://h/dwaas-core/odata/v4/consumption/relational/SAP_S4H/SAP.TIME.VIEW_DIMENSION_YEAR/$metadata",
        "assetRelationalDataUrl": "https://h/dwaas-core/odata/v4/consumption/relational/SAP_S4H/SAP.TIME.VIEW_DIMENSION_YEAR/",
        "assetAnalyticalMetadataUrl": None,
        "assetAnalyticalDataUrl": None,
        "supportsAnalyticalQueries": False,
        "hasParameters": False,
    },
    {"label": "no-name-skipped"},
]


def test_parse_catalog_spaces():
    spaces = parse_catalog_spaces(SPACES)
    assert [s.id for s in spaces] == ["SAP_S4H", "USR0569094"]
    assert spaces[0].name == "SAP S/4HANA Content (BDC)"


def test_parse_catalog_assets_and_entity_set():
    datasets = parse_catalog_assets(ASSETS, "SAP_S4H")
    assert len(datasets) == 1
    d = datasets[0]
    assert d.asset_id == "SAP.TIME.VIEW_DIMENSION_YEAR"
    assert d.entity_set == "SAP_TIME_VIEW_DIMENSION_YEAR"  # dots -> underscores, auto-derived
    assert d.name == "Time Dimension - Year"
    assert d.relational_data_url.endswith("/SAP.TIME.VIEW_DIMENSION_YEAR/")
    assert d.analytical_metadata_url is None and d.analytical_data_url is None
    assert d.supports_analytical is False


def test_entity_set_autoderived_on_manual_dataset():
    d = Dataset(space="S", asset_id="sap.s.My_View", name="x")
    assert d.entity_set == "sap_s_My_View"
