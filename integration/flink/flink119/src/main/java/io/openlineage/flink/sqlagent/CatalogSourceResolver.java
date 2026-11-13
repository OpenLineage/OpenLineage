/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import static io.openlineage.flink.sqlagent.PlannerReflection.call;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resolves a table's source from the Flink catalog used by its SQL plan. */
final class CatalogSourceResolver {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogSourceResolver.class);

  private CatalogSourceResolver() {}

  static void enrich(Object environment, Map<String, Object> datasets) {
    Map<String, Optional<String>> sources = new HashMap<>();
    enrichGroup(environment, datasets.get("inputs"), sources);
    enrichGroup(environment, datasets.get("outputs"), sources);
  }

  @SuppressWarnings("unchecked")
  private static void enrichGroup(
      Object environment, Object group, Map<String, Optional<String>> sources) {
    for (Map<String, Object> dataset : (List<Map<String, Object>>) group) {
      String alias = (String) dataset.remove("flinkCatalog");
      if (alias == null) continue;
      sources
          .computeIfAbsent(alias, key -> sourceUri(environment, key))
          .ifPresent(uri -> dataset.put("dataSourceUri", uri));
    }
  }

  private static Optional<String> sourceUri(Object environment, String alias) {
    try {
      Optional<?> catalog = (Optional<?>) call(environment, "getCatalog", alias);
      if (!catalog.isPresent()) return Optional.empty();
      Object source = catalog.get();
      Optional<String> originalUri = uriFromOriginalProperties(source);
      if (originalUri.isPresent()) return originalUri;
      Optional<String> hiveConfUri = uriFromHiveConf(source);
      if (hiveConfUri.isPresent()) return hiveConfUri;
      if (PlannerReflection.has(source, "catalog")) source = call(source, "catalog");
      if (!PlannerReflection.has(source, "properties")) return Optional.empty();
      return uriFromProperties((Map<?, ?>) call(source, "properties"));
    } catch (Exception error) {
      LOG.debug("Could not resolve source URI for Flink catalog {}", alias, error);
      return Optional.empty();
    }
  }

  private static Optional<String> uriFromOriginalProperties(Object catalog) throws Exception {
    java.lang.reflect.Field field;
    try {
      field = catalog.getClass().getDeclaredField("catalogProps");
    } catch (NoSuchFieldException ignored) {
      return Optional.empty();
    }
    field.setAccessible(true);
    return uriFromProperties((Map<?, ?>) field.get(catalog));
  }

  private static Optional<String> uriFromHiveConf(Object catalog) throws Exception {
    if (!PlannerReflection.has(catalog, "getHiveConf")) return Optional.empty();
    Object conf = call(catalog, "getHiveConf");
    if (conf == null) return Optional.empty();
    Object uris = call(conf, "get", "hive.metastore.uris");
    return uris instanceof String
        ? uriFromProperties(java.util.Collections.singletonMap("hive.metastore.uris", uris))
        : Optional.empty();
  }

  private static Optional<String> uriFromProperties(Map<?, ?> properties) {
    Object value = properties.get("uri");
    if (!(value instanceof String)) value = properties.get("hive.metastore.uris");
    if (!(value instanceof String)) value = properties.get("metastore.uris");
    if (!(value instanceof String)) return Optional.empty();
    URI uri = URI.create(((String) value).split(",", 2)[0].trim());
    return uri.getScheme() != null && uri.getHost() != null
        ? Optional.of(uri.toString())
        : Optional.empty();
  }
}
