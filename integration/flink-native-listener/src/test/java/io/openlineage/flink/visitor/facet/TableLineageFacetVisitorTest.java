/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.flink.client.OpenLineageContext;
import io.openlineage.flink.client.Versions;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.planner.lineage.TableLineageDataset;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

/** Test class for {@link TableLineageFacetVisitor} */
class TableLineageFacetVisitorTest {
  OpenLineageContext context = mock(OpenLineageContext.class);
  TableLineageFacetVisitor visitor = new TableLineageFacetVisitor(context);
  TableLineageDataset table = mock(TableLineageDataset.class, Answers.RETURNS_DEEP_STUBS);
  OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  @Test
  void testApply() {
    when(context.getOpenLineage()).thenReturn(openLineage);
    DatasetFacetsBuilder facetsBuilder = openLineage.newDatasetFacetsBuilder();
    Schema schema = mock(Schema.class);
    when(table.table().getUnresolvedSchema()).thenReturn(schema);
    DataType dataType1 = mock(DataType.class);
    DataType dataType2 = mock(DataType.class);
    when(dataType1.toString()).thenReturn("type1");
    when(dataType2.toString()).thenReturn("type2");

    when(schema.getColumns())
        .thenReturn(
            Arrays.asList(
                new UnresolvedPhysicalColumn("col_a", dataType1),
                new UnresolvedPhysicalColumn("col_b", dataType2)));

    visitor.apply(table, facetsBuilder);

    List<SchemaDatasetFacetFields> fields = facetsBuilder.build().getSchema().getFields();

    assertThat(fields).hasSize(2);
    assertThat(fields.get(0))
        .hasFieldOrPropertyWithValue("name", "col_a")
        .hasFieldOrPropertyWithValue("type", "type1");
    assertThat(fields.get(1))
        .hasFieldOrPropertyWithValue("name", "col_b")
        .hasFieldOrPropertyWithValue("type", "type2");
  }
}
