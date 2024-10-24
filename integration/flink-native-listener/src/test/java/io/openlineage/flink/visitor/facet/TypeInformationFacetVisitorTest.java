/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.flink.client.OpenLineageContext;
import io.openlineage.flink.client.Versions;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.connector.kafka.lineage.facets.TypeInformationFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link TypeInformationFacetVisitor} class. */
class TypeInformationFacetVisitorTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  TypeInformationFacetVisitor facetVisitor = new TypeInformationFacetVisitor(context);
  TypeInformationFacet typeInformationFacet = mock(TypeInformationFacet.class);
  OpenLineage.DatasetFacetsBuilder builder =
      new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI).newDatasetFacetsBuilder();
  LineageDataset dataset = mock(LineageDataset.class);

  @BeforeEach
  public void beforeEach() {
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
  }

  @Test
  void testTestIsDefined() {
    when(dataset.facets()).thenReturn(Map.of("f1", mock(LineageDatasetFacet.class)));
    assertThat(facetVisitor.isDefinedAt(dataset)).isFalse();

    when(dataset.facets()).thenReturn(Map.of("f1", typeInformationFacet));
    assertThat(facetVisitor.isDefinedAt(dataset)).isTrue();
  }

  @Test
  void testFieldsExtractedFromGenericTypeInfo() {
    GenericTypeInfo genericTypeInfo = new GenericTypeInfo(TestingTypeClass.class);
    when(typeInformationFacet.getTypeInformation()).thenReturn(genericTypeInfo);
    when(dataset.facets()).thenReturn(Map.of("typeInformation", typeInformationFacet));

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
  void testNothingHappensForUnsupportedTypeInfo() {
    when(typeInformationFacet.getTypeInformation()).thenReturn(mock(TypeInformation.class));
    when(dataset.facets()).thenReturn(Map.of("typeInformation", typeInformationFacet));

    facetVisitor.apply(dataset, builder);
    assertThat(builder.build().getSchema()).isNull();
  }

  private static class TestingTypeClass {
    public String fieldA;

    @SuppressWarnings("PMD.UnusedPrivateField")
    private String fieldB; // shouldn't be present in schema facet

    public Long fieldC;
  }
}
