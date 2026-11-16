/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.integration.identity;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class JobIdentityTest {
  private static final String RUN_ID = "10bb8b55-39e9-4447-9955-c20a1ab2dd2e";
  private static final String REGISTRY =
      "{\"sources\":[{\"id\":\"hms\",\"namespace\":\"hive://hms:9083\","
          + "\"aliases\":[\"thrift://hms:9083\"],\"managedTables\":[{\"name\":\"db.orders\","
          + "\"metalake\":\"managed\",\"catalog\":\"hive_catalog\"}]},"
          + "{\"id\":\"other\",\"namespace\":\"hive://other:9083\","
          + "\"aliases\":[\"thrift://other:9083\"],\"managedTables\":[{\"name\":\"db.summary\","
          + "\"metalake\":\"other_managed\",\"catalog\":\"other_catalog\"}]},"
          + "{\"id\":\"external\",\"namespace\":\"hive://external:9083\","
          + "\"aliases\":[\"thrift://external:9083\"]}]}";

  @Test
  void preservesEngineJobIdentityWhileSharingManagedDatasetIdentity() throws Exception {
    for (String engine : new String[] {"spark", "hive", "flink"}) {
      DatasetIdentityNormalizer normalizer = DatasetIdentityNormalizer.fromJson(REGISTRY);
      ObjectNode start = event("START", engine + "-jobs");
      normalize(normalizer, start);
      ObjectNode complete = event("COMPLETE", engine + "-jobs");
      dataset(complete, "outputs", "thrift://hms:9083", "db.orders");
      JsonNode result = normalize(normalizer, complete);
      assertThat(result.path("outputs").get(0).path("namespace").asText()).isEqualTo("managed");
      assertThat(result.path("outputs").get(0).path("name").asText())
          .isEqualTo("hive_catalog.db.orders");
    }
  }

  @Test
  void preservesJobNamespaceForReadOnlyAndExternalTables() throws Exception {
    for (String direction : new String[] {"inputs", "outputs"}) {
      for (String source : new String[] {"hms", "external"}) {
        ObjectNode event = event("COMPLETE", "spark-jobs");
        dataset(event, direction, "thrift://" + source + ":9083", "db.orders");
        JsonNode result = normalize(DatasetIdentityNormalizer.fromJson(REGISTRY), event);
        assertThat(result.path(direction).get(0).path("namespace").asText())
            .isEqualTo(source.equals("hms") ? "managed" : "hive://external:9083");
      }
    }
  }

  @Test
  void keepsOneJobAcrossMetalakesAndExternalSourcesWithColumnLineage() throws Exception {
    ObjectNode event = event("COMPLETE", "spark-jobs");
    dataset(event, "inputs", "thrift://hms:9083", "db.orders");
    dataset(event, "inputs", "thrift://external:9083", "db.payments");
    dataset(event, "inputs", "kafka://broker:9092", "orders_topic");
    ObjectNode output = dataset(event, "outputs", "thrift://other:9083", "db.summary");
    ObjectNode lineage = output.putObject("facets").putObject("columnLineage");
    ObjectNode inputField = OpenLineageClientUtils.newObjectMapper().createObjectNode();
    inputField.put("namespace", "thrift://hms:9083").put("name", "db.orders").put("field", "id");
    lineage.putArray("dataset").add(inputField.deepCopy());
    lineage.putObject("fields").putObject("id").putArray("inputFields").add(inputField);

    JsonNode result = normalize(DatasetIdentityNormalizer.fromJson(REGISTRY), event);
    assertThat(result.path("inputs").get(0).path("namespace").asText()).isEqualTo("managed");
    assertThat(result.path("inputs").get(1).path("namespace").asText())
        .isEqualTo("hive://external:9083");
    assertThat(result.path("inputs").get(2).path("namespace").asText())
        .isEqualTo("kafka://broker:9092");
    assertThat(result.path("outputs").get(0).path("namespace").asText()).isEqualTo("other_managed");
    JsonNode normalizedLineage = result.path("outputs").get(0).path("facets").path("columnLineage");
    for (JsonNode field :
        new JsonNode[] {
          normalizedLineage.path("dataset").get(0),
          normalizedLineage.path("fields").path("id").path("inputFields").get(0)
        }) {
      assertThat(field.path("namespace").asText()).isEqualTo("managed");
      assertThat(field.path("name").asText()).isEqualTo("hive_catalog.db.orders");
    }
  }

  @Test
  void emitsLifecycleEventsImmediatelyWithoutDatasets() throws Exception {
    for (String terminal : new String[] {"COMPLETE", "FAIL", "ABORT"}) {
      DatasetIdentityNormalizer normalizer = DatasetIdentityNormalizer.fromJson(REGISTRY);
      normalize(normalizer, event("START", "flink-jobs"));
      for (int i = 0; i < 129; i++) normalize(normalizer, event("RUNNING", "flink-jobs"));
      normalize(normalizer, event(terminal, "flink-jobs"));
    }
  }

  @Test
  void keepsLogicalJobStableWhenDatasetsChangeBetweenEventsAndRuns() throws Exception {
    DatasetIdentityNormalizer normalizer = DatasetIdentityNormalizer.fromJson(REGISTRY);
    for (int execution = 0; execution < 2; execution++) {
      String runId = execution == 0 ? RUN_ID : UUID.randomUUID().toString();
      for (String type : new String[] {"START", "RUNNING", "COMPLETE"}) {
        ObjectNode event = event(type, "spark-jobs");
        event.with("run").put("runId", runId);
        if (!type.equals("COMPLETE")) {
          dataset(event, "inputs", "thrift://hms:9083", "db.orders");
        } else {
          dataset(event, "outputs", "thrift://other:9083", "db.summary");
        }
        normalize(normalizer, event);
      }
    }
  }

  @Test
  void preservesDefaultAndDistinctConfiguredJobNamespaces() throws Exception {
    DatasetIdentityNormalizer normalizer = DatasetIdentityNormalizer.fromJson(REGISTRY);
    for (String namespace : new String[] {"default", "spark-jobs", "hive-jobs", "flink-jobs"}) {
      ObjectNode event = event("COMPLETE", namespace);
      event.with("run").put("runId", UUID.randomUUID().toString());
      dataset(event, "outputs", "thrift://hms:9083", "db.orders");
      normalize(normalizer, event);
    }
  }

  @Test
  void preservesActualSchedulerParentAndRootReferences() throws Exception {
    DatasetIdentityNormalizer normalizer = DatasetIdentityNormalizer.fromJson(REGISTRY);
    ObjectNode parent = event("START", "airflow-jobs");
    dataset(parent, "outputs", "thrift://hms:9083", "db.orders");
    normalize(normalizer, parent);

    ObjectNode child = event("COMPLETE", "spark-jobs");
    child.with("run").put("runId", UUID.randomUUID().toString());
    ObjectNode facet = child.with("run").putObject("facets").putObject("parent");
    facet.put("_producer", "https://openlineage.io");
    facet.put("_schemaURL", "https://openlineage.io/spec/facets/1-0-1/ParentRunFacet.json");
    facet.putObject("job").put("namespace", "airflow-jobs").put("name", "orders");
    facet.putObject("run").put("runId", RUN_ID);
    ObjectNode root = facet.putObject("root");
    root.putObject("job").put("namespace", "airflow-jobs").put("name", "daily_orders");
    root.putObject("run").put("runId", UUID.randomUUID().toString());
    dataset(child, "outputs", "thrift://hms:9083", "db.orders");
    normalize(normalizer, child);
    // References must remain intact even in a different producer which has not seen the parent.
    normalize(DatasetIdentityNormalizer.fromJson(REGISTRY), child);
  }

  @Test
  void normalizesRetriesWithoutChangingJobRunOrEventIdentity() throws Exception {
    DatasetIdentityNormalizer normalizer = DatasetIdentityNormalizer.fromJson(REGISTRY);
    ObjectNode event = event("COMPLETE", "spark-jobs");
    dataset(event, "outputs", "thrift://hms:9083", "db.orders");
    JsonNode first = normalize(normalizer, event);
    assertThat(normalize(normalizer, event)).isEqualTo(first);
    assertThat(normalize(DatasetIdentityNormalizer.fromJson(REGISTRY), event)).isEqualTo(first);
  }

  private static JsonNode normalize(DatasetIdentityNormalizer normalizer, ObjectNode event)
      throws Exception {
    JsonNode original = event.deepCopy();
    OpenLineage.RunEvent input = OpenLineageClientUtils.runEventFromJson(event.toString());
    JsonNode serializedInput = json(OpenLineageClientUtils.toJson(input));
    List<OpenLineage.RunEvent> results = normalizer.normalizeEvents(input, null);
    assertThat(results).hasSize(1);
    JsonNode result = json(OpenLineageClientUtils.toJson(results.get(0)));
    for (String identity :
        new String[] {"job", "run", "eventTime", "eventType", "producer", "schemaURL"}) {
      assertThat(result.path(identity)).as(identity).isEqualTo(serializedInput.path(identity));
    }
    assertThat(event).isEqualTo(original);
    return result;
  }

  private static ObjectNode event(String type, String namespace) throws Exception {
    ObjectNode event =
        (ObjectNode)
            json(
                "{\"eventType\":\""
                    + type
                    + "\",\"eventTime\":\"2026-11-04T00:00:00Z\","
                    + "\"producer\":\"https://openlineage.io\","
                    + "\"schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent\","
                    + "\"run\":{\"runId\":\""
                    + RUN_ID
                    + "\"},"
                    + "\"job\":{\"namespace\":\""
                    + namespace
                    + "\",\"name\":\"orders\"}}");
    return event;
  }

  private static ObjectNode dataset(
      ObjectNode event, String direction, String namespace, String name) {
    return event.withArray(direction).addObject().put("namespace", namespace).put("name", name);
  }

  private static JsonNode json(String value) throws Exception {
    return OpenLineageClientUtils.newObjectMapper().readTree(value);
  }
}
