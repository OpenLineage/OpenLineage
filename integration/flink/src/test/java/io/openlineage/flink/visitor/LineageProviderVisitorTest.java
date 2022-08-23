/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.DatasetFactory;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.EventEmitter;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LineageProviderVisitorTest {
  OpenLineageContext context = mock(OpenLineageContext.class);
  LineageProviderVisitor<OpenLineage.InputDataset> visitor;
  OpenLineage openLineage = new OpenLineage(EventEmitter.OPEN_LINEAGE_CLIENT_URI);

  @BeforeEach
  @SneakyThrows
  public void setup() {
    when(context.getOpenLineage()).thenReturn(openLineage);
    visitor = new LineageProviderVisitor<>(context, DatasetFactory.input(openLineage));
  }

  @Test
  void testIsDefined() {
    assertFalse(visitor.isDefinedAt(mock(Object.class)));
    assertTrue(visitor.isDefinedAt(mock(ExampleLineageProvider.class)));
  }

  @Test
  void testApply() {
    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        openLineage.newSchemaDatasetFacet(
            Collections.singletonList(
                openLineage.newSchemaDatasetFacetFields("a", "INTEGER", "desc")));
    ExampleLineageProvider provider =
        new ExampleLineageProvider("name", "namespace", schemaDatasetFacet);

    List<OpenLineage.InputDataset> facets = visitor.apply(provider);

    assertThat(facets).hasSize(1);

    OpenLineage.Dataset facet = facets.get(0);

    assertThat(facet.getName()).isEqualTo("name");
    assertThat(facet.getNamespace()).isEqualTo("namespace");
    assertThat(facet.getFacets().getSchema().getFields().size()).isEqualTo(1);
    assertThat(facet.getFacets().getSchema().getFields().get(0))
        .isEqualTo(schemaDatasetFacet.getFields().get(0));
  }
}
