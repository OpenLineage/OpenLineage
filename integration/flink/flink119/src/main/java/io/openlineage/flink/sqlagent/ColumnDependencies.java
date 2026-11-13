/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import java.util.*;

/** Connector-neutral field dependency model, shared by planner rules and facet serialization. */
final class ColumnDependencies {
  private final Map<List<String>, Map<String, Object>> references = new LinkedHashMap<>();

  @SuppressWarnings("unchecked")
  void add(Map<String, Object> reference, String type, String subtype, String description) {
    List<String> key =
        Arrays.asList(
            (String) reference.get("namespace"),
            (String) reference.get("name"),
            (String) reference.get("field"));
    Map<String, Object> field =
        references.computeIfAbsent(
            key,
            ignored -> {
              Map<String, Object> copy = new LinkedHashMap<>(reference);
              copy.put("transformations", new ArrayList<Map<String, Object>>());
              return copy;
            });
    Map<String, Object> transformation = new LinkedHashMap<>();
    transformation.put("type", type);
    transformation.put("subtype", subtype);
    if (description != null) transformation.put("description", description);
    // Do not claim a masking classification for arbitrary SQL functions/UDFs.
    List<Map<String, Object>> transformations =
        (List<Map<String, Object>>) field.get("transformations");
    if (!transformations.contains(transformation)) transformations.add(transformation);
  }

  @SuppressWarnings("unchecked")
  void merge(ColumnDependencies other) {
    for (Map<String, Object> ref : other.values()) {
      for (Map<String, Object> t : (List<Map<String, Object>>) ref.get("transformations")) {
        add(ref, (String) t.get("type"), (String) t.get("subtype"), (String) t.get("description"));
      }
    }
  }

  @SuppressWarnings("unchecked")
  ColumnDependencies transform(String type, String subtype, String description) {
    ColumnDependencies result = new ColumnDependencies();
    for (Map<String, Object> ref : values()) {
      boolean direct = false;
      if (type.equals("DIRECT")) {
        for (Map<String, Object> previous :
            (List<Map<String, Object>>) ref.get("transformations")) {
          boolean isDirect = "DIRECT".equals(previous.get("type"));
          direct |= isDirect;
          if (!isDirect || !"IDENTITY".equals(previous.get("subtype"))) {
            result.add(
                ref,
                (String) previous.get("type"),
                (String) previous.get("subtype"),
                (String) previous.get("description"));
          }
        }
      }
      if (!type.equals("DIRECT") || direct) result.add(ref, type, subtype, description);
    }
    return result;
  }

  List<Map<String, Object>> values() {
    return new ArrayList<>(references.values());
  }
}
