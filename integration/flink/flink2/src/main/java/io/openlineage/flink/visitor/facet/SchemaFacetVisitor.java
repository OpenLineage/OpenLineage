/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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
import org.apache.flink.streaming.api.lineage.DatasetSchemaFacet;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

/**
 * Extracts {@link DatasetSchemaFacet} from any dataset that provides it via Flink's lineage API.
 */
public class SchemaFacetVisitor implements DatasetFacetVisitor {

  private final OpenLineageContext context;

  public SchemaFacetVisitor(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDefinedAt(LineageDatasetWithIdentifier dataset) {
    if (new TableLineageDatasetWrapper(dataset.getFlinkDataset()).isTableLineageDataset()) {
      return false;
    }

    return dataset.getFlinkDataset().facets().values().stream()
        .anyMatch(f -> f instanceof DatasetSchemaFacet);
  }

  @Override
  public void apply(LineageDatasetWithIdentifier dataset, DatasetFacetsBuilder builder) {
    for (LineageDatasetFacet facet : dataset.getFlinkDataset().facets().values()) {
      if (facet instanceof DatasetSchemaFacet) {
        DatasetSchemaFacet schemaFacet = (DatasetSchemaFacet) facet;
        List<SchemaDatasetFacetFields> fields =
            schemaFacet.fields().values().stream()
                .map(
                    f ->
                        context
                            .getOpenLineage()
                            .newSchemaDatasetFacetFieldsBuilder()
                            .name(f.name())
                            .type(String.valueOf(f.type()))
                            .build())
                .collect(Collectors.toList());
        builder.schema(context.getOpenLineage().newSchemaDatasetFacet(fields));
      }
    }
  }
}
