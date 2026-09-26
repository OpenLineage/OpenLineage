/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.Versions;
import io.openlineage.flink.converter.LineageDatasetWithIdentifier;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.lineage.DatasetSchemaFacet;
import org.apache.flink.streaming.api.lineage.DatasetSchemaField;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.table.planner.lineage.TableLineageDataset;
import org.junit.jupiter.api.Test;

/** Tests for {@link SchemaFacetVisitor}. */
class SchemaFacetVisitorTest {
  OpenLineageContext context = mock(OpenLineageContext.class);
  SchemaFacetVisitor visitor = new SchemaFacetVisitor(context);
  OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  LineageDataset flinkDataset = mock(LineageDataset.class);
  LineageDatasetWithIdentifier dataset = mock(LineageDatasetWithIdentifier.class);

  @Test
  void testIsDefinedAt() {
    when(dataset.getFlinkDataset()).thenReturn(flinkDataset);
    when(flinkDataset.facets()).thenReturn(Map.of("facet", mock(LineageDatasetFacet.class)));

    assertThat(visitor.isDefinedAt(dataset)).isFalse();

    when(flinkDataset.facets()).thenReturn(Map.of("schema", new TestingDatasetSchemaFacet()));

    assertThat(visitor.isDefinedAt(dataset)).isTrue();

    TableLineageDataset tableDataset = mock(TableLineageDataset.class);
    when(dataset.getFlinkDataset()).thenReturn(tableDataset);
    when(tableDataset.facets()).thenReturn(Map.of("schema", new TestingDatasetSchemaFacet()));

    assertThat(visitor.isDefinedAt(dataset)).isFalse();
  }

  @Test
  void testApply() {
    when(context.getOpenLineage()).thenReturn(openLineage);
    when(dataset.getFlinkDataset()).thenReturn(flinkDataset);
    DatasetFacetsBuilder facetsBuilder = openLineage.newDatasetFacetsBuilder();

    TestingDatasetSchemaFacet schemaFacet = new TestingDatasetSchemaFacet();
    schemaFacet.fields.put("fieldA", new TestingDatasetSchemaField("fieldA", "STRING"));
    schemaFacet.fields.put("fieldB", new TestingDatasetSchemaField("fieldB", "BIGINT"));
    when(flinkDataset.facets()).thenReturn(Map.of("schema", schemaFacet));

    visitor.apply(dataset, facetsBuilder);

    List<SchemaDatasetFacetFields> fields = facetsBuilder.build().getSchema().getFields();
    assertThat(fields).hasSize(2);
    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "fieldA")
        .hasFieldOrPropertyWithValue("type", "STRING");
    assertThat(fields.get(1))
        .hasFieldOrPropertyWithValue("name", "fieldB")
        .hasFieldOrPropertyWithValue("type", "BIGINT");
  }

  private static class TestingDatasetSchemaFacet implements DatasetSchemaFacet {
    private final Map<String, DatasetSchemaField<String>> fields = new LinkedHashMap<>();

    @Override
    public String name() {
      return "schema";
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> Map<String, DatasetSchemaField<T>> fields() {
      return (Map) fields;
    }
  }

  private static class TestingDatasetSchemaField implements DatasetSchemaField<String> {
    private final String name;
    private final String type;

    private TestingDatasetSchemaField(String name, String type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String type() {
      return type;
    }
  }
}
