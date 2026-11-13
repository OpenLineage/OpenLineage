/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** OpenLineage standard dataset facets for SQL metadata. */
final class DatasetFacets {
  static final String PRODUCER = "https://github.com/apache/flink";

  static Map<String, Object> facet(String schema) {
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("_producer", PRODUCER);
    value.put("_schemaURL", "https://openlineage.io/spec/facets/1-" + schema);
    return value;
  }

  static Map<String, Object> dataset(
      String namespace, String name, List<Map<String, Object>> fields) {
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("namespace", namespace);
    value.put("name", name);
    Map<String, Object> schema = facet("1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet");
    schema.put("fields", fields);
    Map<String, Object> facets = new LinkedHashMap<>();
    facets.put("schema", schema);
    value.put("facets", facets);
    return value;
  }

  static Map<String, Object> field(String name, String type) {
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("name", name);
    value.put("type", type);
    return value;
  }

  static Map<String, Object> reference(String namespace, String table, String field) {
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("namespace", namespace);
    value.put("name", table);
    value.put("field", field);
    return value;
  }

  @SuppressWarnings("unchecked")
  static void columns(
      Map<String, Object> dataset, Map<String, List<Map<String, Object>>> dependencies) {
    columns(dataset, dependencies, java.util.Collections.emptyList());
  }

  @SuppressWarnings("unchecked")
  static void columns(
      Map<String, Object> dataset,
      Map<String, List<Map<String, Object>>> dependencies,
      List<Map<String, Object>> indirect) {
    Map<String, Object> facet =
        facet("2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet");
    Map<String, Object> fields = new LinkedHashMap<>();
    dependencies.forEach(
        (name, refs) -> {
          Map<String, Object> field = new LinkedHashMap<>();
          field.put("inputFields", new ArrayList<>(refs));
          fields.put(name, field);
        });
    facet.put("fields", fields);
    facet.put("dataset", new ArrayList<>(indirect));
    ((Map<String, Object>) dataset.get("facets")).put("columnLineage", facet);
  }
}
