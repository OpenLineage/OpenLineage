/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.Versions;
import io.openlineage.flink.converter.LineageDatasetWithIdentifier;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.connector.kafka.lineage.DefaultTypeDatasetFacet;
import org.apache.flink.connector.kafka.lineage.TypeDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link TypeDatasetFacetVisitor} class. */
class TypeDatasetFacetVisitorTest {
  OpenLineageContext context = mock(OpenLineageContext.class);
  TypeDatasetFacetVisitor facetVisitor = new TypeDatasetFacetVisitor(context);
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
  void testTestIsDefined() {
    when(flinkDataset.facets()).thenReturn(Map.of("f1", mock(LineageDatasetFacet.class)));
    assertThat(facetVisitor.isDefinedAt(dataset)).isFalse();

    TypeDatasetFacet typeDatasetFacet = mock(TypeDatasetFacet.class, RETURNS_DEEP_STUBS);
    when(typeDatasetFacet.getTypeInformation().getTypeClass()).thenReturn(PojoTypeInfo.class);
    when(flinkDataset.facets()).thenReturn(Map.of("type", typeDatasetFacet));
    assertThat(facetVisitor.isDefinedAt(dataset)).isTrue();

    when(typeDatasetFacet.getTypeInformation().getTypeClass()).thenReturn(GenericTypeInfo.class);
    when(flinkDataset.facets()).thenReturn(Map.of("type", typeDatasetFacet));
    assertThat(facetVisitor.isDefinedAt(dataset)).isTrue();
  }

  @Test
  void testFieldsExtractedFromGenericTypeInfo() {
    GenericTypeInfo genericTypeInfo = new GenericTypeInfo(TestingTypeClass.class);
    TypeDatasetFacet facet = new DefaultTypeDatasetFacet(genericTypeInfo);
    when(flinkDataset.facets()).thenReturn(Map.of("type", facet));

    facetVisitor.apply(dataset, builder);
    List<SchemaDatasetFacetFields> fields = builder.build().getSchema().getFields();
    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "fieldA")
        .hasFieldOrPropertyWithValue("type", "String");

    assertThat(fields.get(1))
        .hasFieldOrPropertyWithValue("name", "fieldC")
        .hasFieldOrPropertyWithValue("type", "Long");
  }

  @Test
  void testFieldsExtractedFromPojoTypeInfo() {
    PojoTypeInfo genericTypeInfo =
        new PojoTypeInfo(TestingTypeClass.class, Collections.emptyList());
    TypeDatasetFacet facet = new DefaultTypeDatasetFacet(genericTypeInfo);
    when(flinkDataset.facets()).thenReturn(Map.of("type", facet));

    facetVisitor.apply(dataset, builder);
    List<SchemaDatasetFacetFields> fields = builder.build().getSchema().getFields();
    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "fieldA")
        .hasFieldOrPropertyWithValue("type", "String");

    assertThat(fields.get(1))
        .hasFieldOrPropertyWithValue("name", "fieldC")
        .hasFieldOrPropertyWithValue("type", "Long");
  }

  public static class TestingTypeClass {
    public String fieldA;

    @SuppressWarnings("PMD.UnusedPrivateField")
    private String fieldB; // shouldn't be present in schema facet

    public Long fieldC;
  }
}
