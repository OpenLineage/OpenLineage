/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.Versions;
import io.openlineage.flink.converter.LineageDatasetWithIdentifier;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.connector.kinesis.lineage.TypeDatasetFacet;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KinesisTypeDatasetFacetVisitorTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  KinesisTypeDatasetFacetVisitor facetVisitor = new KinesisTypeDatasetFacetVisitor(context);
  OpenLineage.DatasetFacetsBuilder builder =
      new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI).newDatasetFacetsBuilder();
  LineageDataset flinkDataset = mock(LineageDataset.class);
  LineageDatasetWithIdentifier dataset = mock(LineageDatasetWithIdentifier.class);

  @BeforeEach
  public void beforeEach() {
    when(dataset.getFlinkDataset()).thenReturn(flinkDataset);
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
  }

  @Test
  void testIsDefinedAt() {
    when(flinkDataset.facets()).thenReturn(Map.of("type", mock(LineageDatasetFacet.class)));
    assertThat(facetVisitor.isDefinedAt(dataset)).isFalse();

    TypeDatasetFacet kinesisTypeFacet =
        new TypeDatasetFacet(new GenericTypeInfo<>(TestingTypeClass.class));
    when(flinkDataset.facets()).thenReturn(Map.of("type", kinesisTypeFacet));
    assertThat(facetVisitor.isDefinedAt(dataset)).isTrue();
  }

  @Test
  void testFieldsExtractedFromGenericTypeInfo() {
    TypeDatasetFacet facet = new TypeDatasetFacet(new GenericTypeInfo<>(TestingTypeClass.class));
    when(flinkDataset.facets()).thenReturn(Map.of("type", facet));

    facetVisitor.apply(dataset, builder);

    List<SchemaDatasetFacetFields> fields = builder.build().getSchema().getFields();
    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "fieldA")
        .hasFieldOrPropertyWithValue("type", "String");
  }

  @Test
  void testAvroSchemaExtracted() {
    Schema avroSchema =
        SchemaBuilder.record("Order")
            .namespace("io.test")
            .fields()
            .requiredString("orderId")
            .requiredDouble("amount")
            .endRecord();
    TypeDatasetFacet facet = new TypeDatasetFacet(new GenericRecordAvroTypeInfo(avroSchema));
    when(flinkDataset.facets()).thenReturn(Map.of("type", facet));

    facetVisitor.apply(dataset, builder);

    List<SchemaDatasetFacetFields> fields = builder.build().getSchema().getFields();
    assertThat(fields).hasSize(2);
    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "orderId")
        .hasFieldOrPropertyWithValue("type", "string");
    assertThat(fields.get(1))
        .hasFieldOrPropertyWithValue("name", "amount")
        .hasFieldOrPropertyWithValue("type", "double");
  }

  public static class TestingTypeClass {
    public String fieldA;
    public int fieldB;
  }
}
