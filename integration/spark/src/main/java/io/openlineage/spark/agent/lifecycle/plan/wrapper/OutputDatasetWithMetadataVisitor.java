package io.openlineage.spark.agent.lifecycle.plan.wrapper;

import com.google.common.collect.ImmutableMap;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import io.openlineage.spark.agent.facets.OutputStatisticsFacet;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.DataWritingCommand;
import org.apache.spark.sql.execution.command.RunnableCommand;
import org.apache.spark.sql.execution.metric.SQLMetric;
import scala.PartialFunction;
import scala.collection.Map;
import scala.runtime.AbstractPartialFunction;

/**
 * Like the {@link OutputDatasetVisitor}, this is a wrapper around {@link LogicalPlan} visitors that
 * converts found {@link io.openlineage.client.OpenLineage.Dataset}s into {@link
 * io.openlineage.client.OpenLineage.OutputDataset}s and also finds output metrics from the plan in
 * order to construct and link the {@link
 * io.openlineage.client.OpenLineage.OutputStatisticsOutputDatasetFacet} for the written {@link
 * io.openlineage.client.OpenLineage.Dataset}
 */
public class OutputDatasetWithMetadataVisitor
    extends AbstractPartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>> {

  private final PartialFunction<LogicalPlan, List<OpenLineage.Dataset>> visitor;
  private final ImmutableMap<Class, Function<Object, Map<String, SQLMetric>>> statisticsCommands =
      ImmutableMap.<Class, Function<Object, Map<String, SQLMetric>>>builder()
          .put(RunnableCommand.class, (c) -> ((RunnableCommand) c).metrics())
          .put(DataWritingCommand.class, (c) -> ((DataWritingCommand) c).metrics())
          .build();

  public OutputDatasetWithMetadataVisitor(
      PartialFunction<LogicalPlan, List<OpenLineage.Dataset>> visitor) {
    this.visitor = visitor;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan logicalPlan) {
    return visitor.isDefinedAt(logicalPlan);
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(LogicalPlan x) {
    OpenLineage ol = new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);

    Map<String, SQLMetric> metrics =
        statisticsCommands.entrySet().stream()
            .filter(e -> e.getKey().isInstance(x))
            .map(e -> e.getValue().apply(x))
            .findFirst()
            .get();
    OpenLineage.OutputStatisticsOutputDatasetFacet outputStats =
        PlanUtils.getOutputStats(ol, metrics);

    return visitor.apply(x).stream()
        .map(
            dataset -> {
              OpenLineage.DatasetFacets datasetFacets = dataset.getFacets();
              OpenLineage.OutputDatasetOutputFacets outputFacet =
                  new OpenLineage.OutputDatasetOutputFacetsBuilder()
                      .outputStatistics(outputStats)
                      .build();
              // For backward compatibility, include the original "stats" facet in the dataset
              // custom facets.
              // This facet is deprecated and will be removed in a future release of the OpenLineage
              // Spark integration.
              if (!datasetFacets.getAdditionalProperties().containsKey("stats")) {
                datasetFacets
                    .getAdditionalProperties()
                    .put(
                        "stats",
                        (OpenLineage.DatasetFacet)
                            new OutputStatisticsFacet(
                                outputFacet.getOutputStatistics().getRowCount(),
                                outputFacet.getOutputStatistics().getSize()));
              }
              return ol.newOutputDatasetBuilder()
                  .name(dataset.getName())
                  .facets(datasetFacets)
                  .namespace(dataset.getNamespace())
                  .outputFacets(outputFacet)
                  .build();
            })
        .collect(Collectors.toList());
  }
}
