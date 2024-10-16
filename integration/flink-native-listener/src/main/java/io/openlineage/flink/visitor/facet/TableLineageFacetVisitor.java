/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.flink.client.OpenLineageContext;
import io.openlineage.flink.util.TableLineageDatasetUtil;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.planner.lineage.TableLineageDataset;

/** Class for extracting facets from TableLineage datasets */
@Slf4j
public class TableLineageFacetVisitor implements DatasetFacetVisitor {
  private final OpenLineageContext context;

  public TableLineageFacetVisitor(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDefinedAt(LineageDataset flinkDataset) {
    return TableLineageDatasetUtil.isOnClasspath() && flinkDataset instanceof TableLineageDataset;
  }

  @Override
  public void apply(LineageDataset flinkDataset, DatasetFacetsBuilder builder) {
    buildSchemaFacet(flinkDataset, builder);
  }

  private void buildSchemaFacet(LineageDataset flinkDataset, DatasetFacetsBuilder builder) {
    TableLineageDataset table = (TableLineageDataset) flinkDataset;

    List<SchemaDatasetFacetFields> datasetFacetFields =
        table.table().getUnresolvedSchema().getColumns().stream()
            .filter(column -> column instanceof UnresolvedPhysicalColumn)
            .map(
                column ->
                    context
                        .getOpenLineage()
                        .newSchemaDatasetFacetFieldsBuilder()
                        .name(column.getName())
                        .description(column.getComment().orElse(""))
                        .type(((UnresolvedPhysicalColumn) column).getDataType().toString())
                        .build())
            .collect(Collectors.toList());

    builder.schema(context.getOpenLineage().newSchemaDatasetFacet(datasetFacetFields));
  }
}
