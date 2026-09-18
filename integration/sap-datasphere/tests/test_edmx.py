# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from app.datasphere.edmx import parse_edmx

# Minimal EDMX resembling SAP Datasphere $metadata (with default EDM namespace + external labels).
SAMPLE = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="ns">
      <EntityType Name="sap_s4h_IL_A_CostCenter">
        <Key>
          <PropertyRef Name="ControllingArea"/>
          <PropertyRef Name="CostCenter"/>
        </Key>
        <Property Name="ControllingArea" Type="Edm.String" MaxLength="4" Nullable="false"/>
        <Property Name="CostCenter" Type="Edm.String" MaxLength="10" Nullable="false"/>
        <Property Name="LastChangedAt" Type="Edm.DateTimeOffset" Nullable="true"/>
      </EntityType>
      <Annotations Target="ns.sap_s4h_IL_A_CostCenter/ControllingArea">
        <Annotation Term="Common.Label" String="Controlling Area"/>
      </Annotations>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def test_parse_edmx_columns_keys_labels():
    schema = parse_edmx(SAMPLE, entity_type_name="sap_s4h_IL_A_CostCenter")
    assert [c.name for c in schema.columns] == ["ControllingArea", "CostCenter", "LastChangedAt"]

    by_name = {c.name: c for c in schema.columns}
    assert by_name["ControllingArea"].type == "Edm.String"
    assert by_name["ControllingArea"].is_key is True
    assert by_name["ControllingArea"].nullable is False
    assert by_name["ControllingArea"].description == "Controlling Area"

    assert by_name["LastChangedAt"].is_key is False
    assert by_name["LastChangedAt"].nullable is True
    assert {c.name for c in schema.key_columns} == {"ControllingArea", "CostCenter"}


def test_parse_edmx_empty_is_safe():
    assert parse_edmx("<edmx:Edmx xmlns:edmx='x'></edmx:Edmx>").columns == []
