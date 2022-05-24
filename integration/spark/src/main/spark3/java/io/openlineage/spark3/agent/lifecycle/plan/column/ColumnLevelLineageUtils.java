package io.openlineage.spark3.agent.lifecycle.plan.column;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructType;

/**
 * Utility functions for detecting column level lineage within {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan}.
 */
@Slf4j
public class ColumnLevelLineageUtils {

  public static Optional<OpenLineage.ColumnLineageDatasetFacet> buildColumnLineageDatasetFacet(
      OpenLineageContext context, StructType outputSchema) {
    if (!context.getQueryExecution().isPresent()
        || context.getQueryExecution().get().optimizedPlan() == null) {
      return Optional.empty();
    }

    ColumnLevelLineageBuilder builder = new ColumnLevelLineageBuilder(outputSchema, context);
    LogicalPlan plan = context.getQueryExecution().get().optimizedPlan();

    ExpressionDependencyCollector.collect(plan, builder);
    OutputFieldsCollector.collect(plan, builder);
    InputFieldsCollector.collect(context, plan, builder);

    OpenLineage.ColumnLineageDatasetFacetBuilder facetBuilder =
        context.getOpenLineage().newColumnLineageDatasetFacetBuilder();

    facetBuilder.fields(builder.build());
    OpenLineage.ColumnLineageDatasetFacet facet = facetBuilder.build();

    if (facet.getFields().getAdditionalProperties().isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(facetBuilder.build());
    }
  }

  public static OpenLineage.OutputDataset rewriteOutputDataset(
      OpenLineage.OutputDataset outputDataset,
      OpenLineage.ColumnLineageDatasetFacet columnLineageDatasetFacet) {
    OpenLineage.DatasetFacetsBuilder datasetFacetsBuilder =
        new OpenLineage.DatasetFacetsBuilder()
            .documentation(outputDataset.getFacets().getDocumentation())
            .version(outputDataset.getFacets().getVersion())
            .schema(outputDataset.getFacets().getSchema())
            .dataSource(outputDataset.getFacets().getDataSource())
            .lifecycleStateChange(outputDataset.getFacets().getLifecycleStateChange())
            .storage(outputDataset.getFacets().getStorage())
            .columnLineage(columnLineageDatasetFacet);

    outputDataset
        .getFacets()
        .getAdditionalProperties()
        .forEach((key, value) -> datasetFacetsBuilder.put(key, value));

    return new OpenLineage.OutputDatasetBuilder()
        .name(outputDataset.getName())
        .namespace(outputDataset.getNamespace())
        .outputFacets(outputDataset.getOutputFacets())
        .facets(datasetFacetsBuilder.build())
        .build();
  }
}
