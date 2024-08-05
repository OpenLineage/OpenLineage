/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import io.openlineage.client.OpenLineage;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;

public class SchemaUtils {
  @Getter
  @EqualsAndHashCode
  static class SchemaRecord {
    private final String name;
    private final String type;

    public SchemaRecord(String name, String type) {
      this.name = name;
      this.type = type;
    }
  }

  public static List<SchemaRecord> mapToSchemaRecord(OpenLineage.SchemaDatasetFacet schema) {
    return schema.getFields().stream()
        .map(field -> new SchemaRecord(field.getName(), field.getType()))
        .collect(Collectors.toList());
  }
}
