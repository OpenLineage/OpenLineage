/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.facets;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public class HivePropertiesFacetBuilder {
  private static final Set<String> DEFAULT_ALLOWED_PROPERTIES =
      new HashSet<>(Arrays.asList("hive.execution.engine", "hive.query.id"));
  private static final String ALLOWED_PROPERTIES_KEY = "hive.openlineage.capturedProperties";
  private final Configuration conf;
  private final Set<String> allowedProperties;

  public HivePropertiesFacetBuilder(Configuration conf) {
    this.conf = conf;
    allowedProperties =
        conf.get(ALLOWED_PROPERTIES_KEY) != null
            ? Arrays.stream(conf.get(ALLOWED_PROPERTIES_KEY).split(",")).collect(Collectors.toSet())
            : DEFAULT_ALLOWED_PROPERTIES;
  }

  public Map<String, Object> getFilteredConf() {
    Map<String, Object> result = new HashMap<>();
    conf.iterator()
        .forEachRemaining(
            entry -> {
              String key = entry.getKey();
              if (allowedProperties.contains(key)) {
                result.putIfAbsent(key, entry.getValue());
              }
            });
    return result;
  }

  public HivePropertiesFacet build() {
    return new HivePropertiesFacet(getFilteredConf());
  }
}
