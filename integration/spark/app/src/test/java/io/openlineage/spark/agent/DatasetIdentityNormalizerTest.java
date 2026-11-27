/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.integration.identity.DatasetIdentityNormalizer;
import org.junit.jupiter.api.Test;

class DatasetIdentityNormalizerTest {
  private static final String REGISTRY =
      "{\"sources\":["
          + "{\"id\":\"hms-prod\",\"aliases\":[\"thrift://hms-prod:9083\",\"thrift://hms-alias:9083\"],"
          + "\"namespace\":\"hive://hms-prod:9083\",\"managedTables\":["
          + "{\"name\":\"test_db.orders\",\"metalake\":\"spark_metalake\",\"catalog\":\"hive_catalog\"}]},"
          + "{\"id\":\"hms-other\",\"aliases\":[\"thrift://hms-other:9083\"],"
          + "\"namespace\":\"hive://hms-other:9083\"}]}";

  @Test
  void rewritesManagedTableAndColumnReferencesWithoutChangingJobOrRun() throws Exception {
    String event =
        "{\"job\":{\"namespace\":\"hive-jobs\",\"name\":\"etl\"},"
            + "\"run\":{\"runId\":\"10bb8b55-39e9-4447-9955-c20a1ab2dd2e\"},"
            + "\"inputs\":[{\"namespace\":\"hive://hms-alias:9083\",\"name\":\"test_db.orders\",\"facets\":{}}],"
            + "\"outputs\":[{\"namespace\":\"hive://hms-prod:9083\",\"name\":\"test_db.summary\","
            + "\"facets\":{\"columnLineage\":{\"dataset\":[{\"namespace\":\"hive://hms-alias:9083\",\"name\":\"test_db.orders\",\"field\":\"id\"}],"
            + "\"fields\":{\"id\":{\"inputFields\":[{\"namespace\":\"hive://hms-alias:9083\",\"name\":\"test_db.orders\",\"field\":\"id\",\"transformations\":[{\"type\":\"DIRECT\"}]}]}}}}}]}";
    JsonNode result =
        OpenLineageClientUtils.newObjectMapper()
            .readTree(DatasetIdentityNormalizer.fromJson(REGISTRY).normalizeJson(event, null));

    assertThat(result.path("job").path("namespace").asText()).isEqualTo("hive-jobs");
    assertThat(result.path("job").path("name").asText()).isEqualTo("etl");
    assertThat(result.path("run").path("runId").asText())
        .isEqualTo("10bb8b55-39e9-4447-9955-c20a1ab2dd2e");
    assertThat(result.path("inputs").get(0).path("namespace").asText()).isEqualTo("spark_metalake");
    assertThat(result.path("inputs").get(0).path("name").asText())
        .isEqualTo("hive_catalog.test_db.orders");
    assertThat(result.path("outputs").get(0).path("namespace").asText())
        .isEqualTo("hive://hms-prod:9083");
    JsonNode lineage = result.path("outputs").get(0).path("facets").path("columnLineage");
    assertThat(lineage.path("dataset").get(0).path("name").asText())
        .isEqualTo("hive_catalog.test_db.orders");
    JsonNode field = lineage.path("fields").path("id").path("inputFields").get(0);
    assertThat(field.path("namespace").asText()).isEqualTo("spark_metalake");
    assertThat(field.path("field").asText()).isEqualTo("id");
    assertThat(field.path("transformations").get(0).path("type").asText()).isEqualTo("DIRECT");
  }

  @Test
  void keepsSameNameInAnotherMetastoreSeparate() throws Exception {
    String event =
        "{\"inputs\":[{\"namespace\":\"hive://hms-other:9083\",\"name\":\"test_db.orders\"}]}";
    JsonNode result =
        OpenLineageClientUtils.newObjectMapper()
            .readTree(DatasetIdentityNormalizer.fromJson(REGISTRY).normalizeJson(event, null));
    assertThat(result.path("inputs").get(0).path("namespace").asText())
        .isEqualTo("hive://hms-other:9083");
  }

  @Test
  void usesHiveMetastoreHintForCatalogNames() throws Exception {
    String event = "{\"inputs\":[{\"namespace\":\"hive\",\"name\":\"test_db.orders\"}]}";
    JsonNode result =
        OpenLineageClientUtils.newObjectMapper()
            .readTree(
                DatasetIdentityNormalizer.fromJson(REGISTRY)
                    .normalizeJson(event, "thrift://hms-prod:9083"));
    assertThat(result.path("inputs").get(0).path("namespace").asText()).isEqualTo("spark_metalake");
  }

  @Test
  void rejectsUnknownTableSource() {
    String event =
        "{\"inputs\":[{\"namespace\":\"hive://unknown:9083\",\"name\":\"test_db.orders\"}]}";
    assertThatThrownBy(
            () -> DatasetIdentityNormalizer.fromJson(REGISTRY).normalizeJson(event, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No authoritative source");
  }

  @Test
  void removesLocalWarehouseAliasesWithoutChangingFileIdentityOrColumnLineage() throws Exception {
    String event =
        "{\"job\":{\"namespace\":\"spark-jobs\",\"name\":\"summary\"},"
            + "\"inputs\":[{\"namespace\":\"file\",\"name\":\"/warehouse/db.db/orders\","
            + "\"facets\":{\"schema\":{\"fields\":[]},\"symlinks\":{\"identifiers\":["
            + "{\"namespace\":\"file:/warehouse\",\"name\":\"db.orders\",\"type\":\"TABLE\"},"
            + "{\"namespace\":\"file:///warehouse\",\"name\":\"db.orders\",\"type\":\"TABLE\"}]}}}],"
            + "\"outputs\":[{\"namespace\":\"file\",\"name\":\"/warehouse/db.db/summary\","
            + "\"facets\":{\"symlinks\":{\"identifiers\":[{\"namespace\":\"file:/warehouse\","
            + "\"name\":\"db.summary\",\"type\":\"TABLE\"}]},\"columnLineage\":{\"fields\":{\"id\":{"
            + "\"inputFields\":[{\"namespace\":\"file\",\"name\":\"/warehouse/db.db/orders\",\"field\":\"id\"}]}}}}}]}";
    DatasetIdentityNormalizer normalizer = DatasetIdentityNormalizer.fromJson(REGISTRY);
    JsonNode original = OpenLineageClientUtils.newObjectMapper().readTree(event);
    JsonNode result =
        OpenLineageClientUtils.newObjectMapper().readTree(normalizer.normalizeJson(event, null));
    for (String direction : new String[] {"inputs", "outputs"}) {
      JsonNode dataset = result.path(direction).get(0);
      assertThat(dataset.path("namespace").asText()).isEqualTo("file");
      assertThat(dataset.path("name")).isEqualTo(original.path(direction).get(0).path("name"));
      assertThat(dataset.path("facets").has("symlinks")).isFalse();
    }
    assertThat(result.path("job")).isEqualTo(original.path("job"));
    assertThat(result.path("inputs").get(0).path("facets").path("schema"))
        .isEqualTo(original.path("inputs").get(0).path("facets").path("schema"));
    assertThat(result.path("outputs").get(0).path("facets").path("columnLineage"))
        .isEqualTo(original.path("outputs").get(0).path("facets").path("columnLineage"));
    assertThat(normalizer.normalizeJson(result.toString(), null)).isEqualTo(result.toString());
  }

  @Test
  void preservesOtherFileLinksAndNonLocalStorageAliases() throws Exception {
    String event =
        "{\"inputs\":[{\"namespace\":\"file\",\"name\":\"/warehouse/db.db/orders\",\"facets\":{"
            + "\"symlinks\":{\"identifiers\":[{\"namespace\":\"file:/warehouse\",\"name\":\"db.orders\",\"type\":\"TABLE\"},"
            + "{\"namespace\":\"file:/archive\",\"name\":\"/orders\",\"type\":\"FILE\"}]}}},"
            + "{\"namespace\":\"hdfs://namenode:8020\",\"name\":\"/warehouse/db.db/orders\",\"facets\":{"
            + "\"symlinks\":{\"identifiers\":[{\"namespace\":\"hdfs://namenode:8020/warehouse\","
            + "\"name\":\"db.orders\",\"type\":\"TABLE\"}]}}}]}";
    JsonNode result =
        OpenLineageClientUtils.newObjectMapper()
            .readTree(DatasetIdentityNormalizer.fromJson(REGISTRY).normalizeJson(event, null));
    JsonNode links =
        result.path("inputs").get(0).path("facets").path("symlinks").path("identifiers");
    assertThat(links.size()).isEqualTo(1);
    assertThat(links.get(0).path("type").asText()).isEqualTo("FILE");
    assertThat(result.path("inputs").get(1))
        .isEqualTo(OpenLineageClientUtils.newObjectMapper().readTree(event).path("inputs").get(1));
  }

  @Test
  void keepsAuthoritativeMetastoreLinkWhenRemovingLocalWarehouseAlias() throws Exception {
    String event =
        "{\"inputs\":[{\"namespace\":\"file\",\"name\":\"/warehouse/test_db.db/orders\",\"facets\":{"
            + "\"symlinks\":{\"identifiers\":[{\"namespace\":\"file:/warehouse\",\"name\":\"test_db.orders\",\"type\":\"TABLE\"},"
            + "{\"namespace\":\"hive://hms-prod:9083\",\"name\":\"test_db.orders\",\"type\":\"TABLE\"}]}}}]}";
    JsonNode result =
        OpenLineageClientUtils.newObjectMapper()
            .readTree(DatasetIdentityNormalizer.fromJson(REGISTRY).normalizeJson(event, null));
    assertThat(result.path("inputs").get(0).path("namespace").asText()).isEqualTo("spark_metalake");
    assertThat(result.path("inputs").get(0).path("name").asText())
        .isEqualTo("hive_catalog.test_db.orders");
  }

  @Test
  void rejectsConflictingCanonicalManagedIdentity() {
    String registry =
        "{\"sources\":["
            + "{\"id\":\"a\",\"namespace\":\"hive://a:9083\",\"aliases\":[\"hive://a:9083\"],\"managedTables\":[{\"name\":\"db.t\",\"metalake\":\"m\",\"catalog\":\"c\"}]},"
            + "{\"id\":\"b\",\"namespace\":\"hive://b:9083\",\"aliases\":[\"hive://b:9083\"],\"managedTables\":[{\"name\":\"db.t\",\"metalake\":\"m\",\"catalog\":\"c\"}]}]}";
    assertThatThrownBy(() -> DatasetIdentityNormalizer.fromJson(registry))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Managed identity");
  }
}
