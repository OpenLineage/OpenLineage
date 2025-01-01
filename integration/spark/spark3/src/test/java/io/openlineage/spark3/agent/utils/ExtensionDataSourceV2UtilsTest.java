/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.dataset.DatasetCompositeFacetsBuilder;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExtensionDataSourceV2UtilsTest {
  static final String DATASET_NAME = "some-dataset-name";
  static final String DATASET_NAMESPACE = "some-dataset-name";

  DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
  Table table = mock(Table.class);
  Map<String, String> properties = new HashMap<>();
  OpenLineage openLineage;
  DatasetCompositeFacetsBuilder datasetFacetsBuilder;
  URI producer;

  @BeforeEach
  void setup() throws URISyntaxException {
    producer = new URI("https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client");
    openLineage = new OpenLineage(producer);
    datasetFacetsBuilder = new DatasetCompositeFacetsBuilder(openLineage);

    when(relation.table()).thenReturn(table);
    when(table.properties()).thenReturn(properties);

    properties.put("openlineage.dataset.name", DATASET_NAME);
    properties.put("openlineage.dataset.namespace", DATASET_NAMESPACE);
  }

  @Test
  void testHasExtensionLineage() {
    assertTrue(ExtensionDataSourceV2Utils.hasExtensionLineage(relation));
    properties.clear();
    assertFalse(ExtensionDataSourceV2Utils.hasExtensionLineage(relation));
  }

  @Test
  void testHasExtensionLineageWhenPropertiesAreNull() {
    when(table.properties()).thenReturn(null);
    assertFalse(ExtensionDataSourceV2Utils.hasExtensionLineage(relation));
  }

  @Test
  void testGetDatasetName() {
    assertThat(ExtensionDataSourceV2Utils.getDatasetIdentifier(relation))
        .hasFieldOrPropertyWithValue("name", DATASET_NAME)
        .hasFieldOrPropertyWithValue("namespace", DATASET_NAMESPACE);
  }

  @Test
  void testLoadBuilderWithGlobalFacet() {
    SchemaDatasetFacet schema =
        openLineage.newSchemaDatasetFacet(
            Collections.singletonList(
                openLineage
                    .newSchemaDatasetFacetFieldsBuilder()
                    .name("user_id")
                    .type("int64")
                    .build()));
    properties.put("openlineage.dataset.facets.schema", OpenLineageClientUtils.toJson(schema));

    ExtensionDataSourceV2Utils.loadBuilder(openLineage, datasetFacetsBuilder, relation);
    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        openLineage.newSchemaDatasetFacet(
            Arrays.asList(
                openLineage
                    .newSchemaDatasetFacetFieldsBuilder()
                    .name("name")
                    .type("user_id")
                    .build(),
                openLineage
                    .newSchemaDatasetFacetFieldsBuilder()
                    .name("type")
                    .type("int64")
                    .build()));

    assertThat(datasetFacetsBuilder.getFacets().build().getSchema().get_producer().toString())
        .isEqualTo("https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client");
    assertThat(datasetFacetsBuilder.getFacets().build().getSchema().get_schemaURL().toString())
        .isEqualTo(schemaDatasetFacet.get_schemaURL().toString());
    assertThat(datasetFacetsBuilder.getFacets().build().getSchema().getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "user_id")
        .hasFieldOrPropertyWithValue("type", "int64");
  }

  @Test
  void testLoadBuilderWithCustomFacet() {
    String facetJson =
        "{\n"
            + "    \"property1\": {\n"
            + "       \"property2\": \"value2\","
            + "       \"property3\": [\"value31\", \"value32\"]"
            + "    },\n"
            + "    \"property4\": \"value4\","
            + "    \"_producer\": \"https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client\",\n"
            + "    \"_schemaURL\": \"https://openlineage.io/spec/facets/1-1-0/SchemaDatasetFacet.json\"\n"
            + "  }";

    properties.put("openlineage.dataset.facets.customFacet", facetJson);

    ExtensionDataSourceV2Utils.loadBuilder(openLineage, datasetFacetsBuilder, relation);

    DatasetFacet datasetFacet =
        datasetFacetsBuilder.getFacets().build().getAdditionalProperties().get("customFacet");

    assertThat(datasetFacet.get_producer()).isEqualTo(producer);
    assertThat(datasetFacet.getAdditionalProperties().get("property4")).isEqualTo("value4");
    assertThat(datasetFacet.getAdditionalProperties().get("property1"))
        .isInstanceOf(Map.class)
        .hasFieldOrPropertyWithValue("property2", "value2")
        .hasFieldOrPropertyWithValue("property3", Arrays.asList("value31", "value32"));
  }

  @Test
  void testLoadBuilderWithCustomFacetWithoutProducer() {
    String facetJson =
        "{\n"
            + "    \"property\": \"value\","
            + "    \"_schemaURL\": \"https://openlineage.io/spec/facets/1-1-0/SchemaDatasetFacet.json\"\n"
            + "  }";

    properties.put("openlineage.dataset.facets.customFacet", facetJson);

    ExtensionDataSourceV2Utils.loadBuilder(openLineage, datasetFacetsBuilder, relation);

    DatasetFacet datasetFacet =
        datasetFacetsBuilder.getFacets().build().getAdditionalProperties().get("customFacet");

    assertThat(datasetFacet.get_producer()).isEqualTo(producer);
  }
}
