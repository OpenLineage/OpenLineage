# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

"""Parse OData v4 EDMX ($metadata) XML into a backend-neutral :class:`DatasetSchema`.

Handles the shape SAP Datasphere returns:
  * ``<EntityType><Key><PropertyRef Name=.../></Key><Property Name=.. Type=.. Nullable=../></EntityType>``
  * business labels via ``<Annotation Term="Common.Label" String=".."/>`` either inline under a
    ``Property`` or in a top-level ``<Annotations Target="NS.Type/Prop">`` block.

Namespaces vary by service, so we match on the *local* tag name (ignoring the XML namespace).
"""

from __future__ import annotations

import xml.etree.ElementTree as ET

from app.datasphere.models import Column, DatasetSchema


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _find_all(root: ET.Element, name: str) -> list[ET.Element]:
    return [el for el in root.iter() if _local(el.tag) == name]


def _label_from_annotation(el: ET.Element) -> str | None:
    term = el.get("Term", "")
    if term.split(".")[-1] != "Label":
        return None
    # Value may be a String attribute or a nested <String>text</String>.
    if el.get("String") is not None:
        return el.get("String")
    for child in el:
        if _local(child.tag) == "String":
            return (child.text or "").strip() or None
    return None


def _collect_labels(root: ET.Element, entity_type: ET.Element) -> dict[str, str]:
    labels: dict[str, str] = {}

    # Inline: <Property Name="X"><Annotation Term="Common.Label" String=".."/></Property>
    for prop in _find_all(entity_type, "Property"):
        pname = prop.get("Name")
        if not pname:
            continue
        for ann in _find_all(prop, "Annotation"):
            label = _label_from_annotation(ann)
            if label:
                labels[pname] = label
                break

    # External: <Annotations Target="NS.Type/Prop"><Annotation Term="Common.Label" .../></Annotations>
    for anns in _find_all(root, "Annotations"):
        target = anns.get("Target", "")
        if "/" not in target:
            continue
        pname = target.rsplit("/", 1)[-1]
        if pname in labels:
            continue
        for ann in _find_all(anns, "Annotation"):
            label = _label_from_annotation(ann)
            if label:
                labels[pname] = label
                break

    return labels


def parse_edmx(xml_text: str, entity_type_name: str | None = None) -> DatasetSchema:
    root = ET.fromstring(xml_text)

    entity_types = _find_all(root, "EntityType")
    if not entity_types:
        return DatasetSchema(columns=[])

    entity_type = None
    if entity_type_name:
        for et in entity_types:
            if et.get("Name") == entity_type_name:
                entity_type = et
                break
    entity_type = entity_type or entity_types[0]

    key_names = {
        ref.get("Name")
        for key in _find_all(entity_type, "Key")
        for ref in _find_all(key, "PropertyRef")
        if ref.get("Name")
    }
    labels = _collect_labels(root, entity_type)

    columns: list[Column] = []
    for prop in _find_all(entity_type, "Property"):
        name = prop.get("Name")
        if not name:
            continue
        columns.append(
            Column(
                name=name,
                type=prop.get("Type", "Edm.String"),
                nullable=prop.get("Nullable", "true").lower() != "false",
                is_key=name in key_names,
                description=labels.get(name),
            )
        )
    return DatasetSchema(columns=columns)
