/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.converter.LineageDatasetWithIdentifier;
import io.openlineage.flink.wrapper.TableLineageDatasetWrapper;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.catalog.CatalogBaseTable;

/** Class for extracting facets from TableLineage datasets */
@Slf4j
public class TableLineageFacetVisitor implements DatasetFacetVisitor {
  private final OpenLineageContext context;

  public TableLineageFacetVisitor(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDefinedAt(LineageDatasetWithIdentifier dataset) {
    return new TableLineageDatasetWrapper(dataset.getFlinkDataset()).isTableLineageDataset();
  }

  @Override
  public void apply(LineageDatasetWithIdentifier dataset, DatasetFacetsBuilder builder) {
    buildSchemaFacet(dataset.getFlinkDataset(), builder);
  }

  private void buildSchemaFacet(LineageDataset flinkDataset, DatasetFacetsBuilder builder) {
    CatalogBaseTable table = new TableLineageDatasetWrapper(flinkDataset).getTable().orElse(null);

    List<SchemaDatasetFacetFields> datasetFacetFields =
        table.getUnresolvedSchema().getColumns().stream()
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

    table
        .getDescription()
        .ifPresent(
            description ->
                builder.documentation(
                    context.getOpenLineage().newDocumentationDatasetFacet(description)));

    builder.schema(context.getOpenLineage().newSchemaDatasetFacet(datasetFacetFields));
  }
}
