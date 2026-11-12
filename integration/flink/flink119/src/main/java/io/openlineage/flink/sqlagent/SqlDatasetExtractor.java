/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import static io.openlineage.flink.sqlagent.PlannerReflection.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Uses the live planner classloader; Calcite is deliberately not bundled in the agent. */
final class SqlDatasetExtractor {
  private final Map<String, String> catalogs = new LinkedHashMap<>();
  private final String metalake;

  SqlDatasetExtractor(String catalog, String alias) {
    this(
        catalog,
        alias,
        legacyMetalake(),
        System.getProperty("flink.lineage.catalog-mapping", "{}"));
  }

  private static String legacyMetalake() {
    String endpoint = System.getProperty("openlineage.transport.endpoint", "");
    String url = System.getProperty("openlineage.transport.url", "");
    return "/api/lineage".equals(endpoint) || url.endsWith("/api/lineage")
        ? ""
        : System.getProperty("flink.lineage.metalake", "");
  }

  SqlDatasetExtractor(String catalog, String alias, String metalake, String mapping) {
    this.metalake = metalake.trim();
    if (!catalog.isEmpty() && !alias.isEmpty()) catalogs.put(alias, catalog);
    try {
      Map<?, ?> configured =
          new com.fasterxml.jackson.databind.ObjectMapper().readValue(mapping, Map.class);
      for (Map.Entry<?, ?> entry : configured.entrySet()) {
        if (!(entry.getValue() instanceof String) || ((String) entry.getValue()).trim().isEmpty()) {
          throw new IllegalArgumentException("Catalog name must be a nonempty string");
        }
        catalogs.put(entry.getKey().toString(), entry.getValue().toString());
      }
    } catch (Exception error) {
      throw new IllegalArgumentException("Invalid flink.lineage.catalog-mapping", error);
    }
  }

  List<String> identity(Object alias, Object schema, Object table) {
    String catalog = catalogs.getOrDefault(alias.toString(), alias.toString());
    String namespace = metalake.isEmpty() ? catalog : metalake;
    String name = (metalake.isEmpty() ? "" : catalog + ".") + schema + "." + table;
    return java.util.Arrays.asList(namespace, name);
  }

  Map<String, Object> extract(Object operations) throws Exception {
    List<Map<String, Object>> outputs = new ArrayList<>();
    Map<List<String>, Map<String, Object>> inputs = new LinkedHashMap<>();
    for (Object operation : (List<?>) operations) {
      if (!operation.getClass().getSimpleName().equals("SinkModifyOperation")) continue;
      Object context = call(operation, "getContextResolvedTable");
      Object identifier = call(context, "getIdentifier");
      List<String> outputIdentity =
          identity(
              call(identifier, "getCatalogName"),
              call(identifier, "getDatabaseName"),
              call(identifier, "getObjectName"));
      Object query = call(operation, "getChild");
      // SQL INSERT queries expose a Calcite tree.
      if (!has(query, "getCalciteTree")) continue;
      Object rel = call(query, "getCalciteTree");
      Object schema = call(call(context, "getResolvedTable"), "getResolvedSchema");
      List<?> names = (List<?>) call(schema, "getColumnNames");
      List<?> types = (List<?>) call(schema, "getColumnDataTypes");
      List<Map<String, Object>> fields = new ArrayList<>();
      for (int i = 0; i < names.size(); i++)
        fields.add(DatasetFacets.field(names.get(i).toString(), types.get(i).toString()));
      Map<String, Object> output =
          DatasetFacets.dataset(outputIdentity.get(0), outputIdentity.get(1), fields);
      output.put("flinkCatalog", call(identifier, "getCatalogName").toString());
      Object metadata = call(call(rel, "getCluster"), "getMetadataQuery");
      collectTables(rel, inputs);
      Map<String, List<Map<String, Object>>> dependencies = new LinkedHashMap<>();
      SqlColumnLineage lineage =
          new SqlColumnLineage(
              metadata,
              origin -> {
                Object table = call(origin, "getOriginTable");
                List<String> input = tableIdentity(table);
                inputs.putIfAbsent(input, tableDataset(table, input));
                int ordinal = (Integer) call(origin, "getOriginColumnOrdinal");
                List<?> sourceFields = (List<?>) call(call(table, "getRowType"), "getFieldList");
                return DatasetFacets.reference(
                    input.get(0),
                    input.get(1),
                    call(sourceFields.get(ordinal), "getName").toString());
              });
      int[][] targets = (int[][]) call(operation, "getTargetColumns");
      int count = ((List<?>) call(call(rel, "getRowType"), "getFieldList")).size();
      // Unassigned sink columns (e.g. INSERT target-column lists) have no source field.
      for (Object name : names) dependencies.put(name.toString(), new ArrayList<>());
      for (int i = 0; i < count; i++) {
        if (targets != null && targets.length > 0 && targets[i].length != 1) {
          throw new IllegalStateException("Nested target columns are not supported");
        }
        int target = targets == null || targets.length == 0 ? i : targets[i][0];
        dependencies.put(names.get(target).toString(), lineage.field(rel, i).values());
      }
      DatasetFacets.columns(output, dependencies, lineage.dataset(rel).values());
      outputs.add(output);
    }
    if (outputs.isEmpty()) return null;
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("inputs", new ArrayList<>(inputs.values()));
    result.put("outputs", outputs);
    return result;
  }

  private void collectTables(Object rel, Map<List<String>, Map<String, Object>> inputs)
      throws Exception {
    if (has(rel, "getTable")) {
      Object table = call(rel, "getTable");
      if (table != null) {
        List<String> identity = tableIdentity(table);
        inputs.putIfAbsent(identity, tableDataset(table, identity));
      }
    }
    for (Object child : (List<?>) call(rel, "getInputs")) collectTables(child, inputs);
  }

  private List<String> tableIdentity(Object table) throws Exception {
    List<?> path = (List<?>) call(table, "getQualifiedName");
    if (path.size() != 3)
      throw new IllegalStateException("Unsupported SQL table identifier: " + path);
    return identity(path.get(0), path.get(1), path.get(2));
  }

  private Map<String, Object> tableDataset(Object table, List<String> identity) throws Exception {
    List<Map<String, Object>> fields = new ArrayList<>();
    for (Object field : (List<?>) call(call(table, "getRowType"), "getFieldList")) {
      fields.add(
          DatasetFacets.field(
              call(field, "getName").toString(), call(field, "getType").toString()));
    }
    Map<String, Object> dataset = DatasetFacets.dataset(identity.get(0), identity.get(1), fields);
    dataset.put("flinkCatalog", pathCatalog(table));
    return dataset;
  }

  private String pathCatalog(Object table) throws Exception {
    return ((List<?>) call(table, "getQualifiedName")).get(0).toString();
  }
}
