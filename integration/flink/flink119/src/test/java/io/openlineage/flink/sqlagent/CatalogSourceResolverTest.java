/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class CatalogSourceResolverTest {
  @Test
  void usesRegisteredCatalogUriForEveryTable() {
    Map<String, Object> input = dataset("gv_iceberg");
    Map<String, Object> output = dataset("gv_iceberg");
    Map<String, Object> datasets = new HashMap<>();
    datasets.put("inputs", Arrays.asList(input));
    datasets.put("outputs", Arrays.asList(output));

    CatalogSourceResolver.enrich(new Environment(), datasets);

    assertEquals("http://catalog.example:9001/iceberg/", input.get("dataSourceUri"));
    assertEquals("http://catalog.example:9001/iceberg/", output.get("dataSourceUri"));
    assertFalse(input.containsKey("flinkCatalog"));
  }

  @Test
  void leavesUnresolvedCatalogWithoutInventedSource() {
    Map<String, Object> input = dataset("missing");
    Map<String, Object> datasets = new HashMap<>();
    datasets.put("inputs", Arrays.asList(input));
    datasets.put("outputs", java.util.Collections.emptyList());

    CatalogSourceResolver.enrich(new Environment(), datasets);

    assertFalse(input.containsKey("dataSourceUri"));
  }

  @Test
  void usesOriginalIcebergCatalogPropertiesWhenRuntimeCatalogOmitsUri() {
    Map<String, Object> input = dataset("gv_iceberg");
    Map<String, Object> datasets = new HashMap<>();
    datasets.put("inputs", Arrays.asList(input));
    datasets.put("outputs", java.util.Collections.emptyList());

    CatalogSourceResolver.enrich(new EnvironmentWithCatalogProperties(), datasets);

    assertEquals("http://catalog.example:9001/iceberg/", input.get("dataSourceUri"));
  }

  @Test
  void readsHiveMetastoreFromCatalogProperties() {
    Map<String, Object> input = dataset("gv_hive");
    Map<String, Object> datasets = new HashMap<>();
    datasets.put("inputs", Arrays.asList(input));
    datasets.put("outputs", java.util.Collections.emptyList());

    CatalogSourceResolver.enrich(new HiveEnvironment(), datasets);

    assertEquals("thrift://hms-prod:9083", input.get("dataSourceUri"));
  }

  @Test
  void readsHiveMetastoreFromHiveConf() {
    Map<String, Object> input = dataset("gv_hive");
    Map<String, Object> datasets = new HashMap<>();
    datasets.put("inputs", Arrays.asList(input));
    datasets.put("outputs", java.util.Collections.emptyList());

    CatalogSourceResolver.enrich(new HiveConfEnvironment(), datasets);

    assertEquals("thrift://hms-prod:9083", input.get("dataSourceUri"));
  }

  private static Map<String, Object> dataset(String catalog) {
    Map<String, Object> dataset = new HashMap<>();
    dataset.put("flinkCatalog", catalog);
    return dataset;
  }

  public static final class Environment {
    public Optional<FlinkCatalog> getCatalog(String name) {
      return "gv_iceberg".equals(name) ? Optional.of(new FlinkCatalog()) : Optional.empty();
    }
  }

  public static final class HiveEnvironment {
    public Optional<HiveCatalog> getCatalog(String name) {
      return Optional.of(new HiveCatalog());
    }
  }

  public static final class HiveCatalog {
    private final Map<String, String> catalogProps =
        java.util.Collections.singletonMap(
            "hive.metastore.uris", "thrift://hms-prod:9083,thrift://hms-alias:9083");
  }

  public static final class HiveConfEnvironment {
    public Optional<HiveConfCatalog> getCatalog(String name) {
      return Optional.of(new HiveConfCatalog());
    }
  }

  public static final class HiveConfCatalog {
    public HiveConf getHiveConf() {
      return new HiveConf();
    }
  }

  public static final class HiveConf {
    public String get(String key) {
      return "hive.metastore.uris".equals(key)
          ? "thrift://hms-prod:9083,thrift://hms-alias:9083"
          : null;
    }
  }

  public static final class FlinkCatalog {
    public RestCatalog catalog() {
      return new RestCatalog();
    }
  }

  public static final class RestCatalog {
    public Map<String, String> properties() {
      return java.util.Collections.singletonMap("uri", "http://catalog.example:9001/iceberg/");
    }
  }

  public static final class EnvironmentWithCatalogProperties {
    public Optional<FlinkCatalogWithProperties> getCatalog(String name) {
      return Optional.of(new FlinkCatalogWithProperties());
    }
  }

  public static final class FlinkCatalogWithProperties {
    private final Map<String, String> catalogProps =
        java.util.Collections.singletonMap("uri", "http://catalog.example:9001/iceberg/");

    public RestCatalogWithoutUri catalog() {
      return new RestCatalogWithoutUri();
    }
  }

  public static final class RestCatalogWithoutUri {
    public Map<String, String> properties() {
      return java.util.Collections.emptyMap();
    }
  }
}
