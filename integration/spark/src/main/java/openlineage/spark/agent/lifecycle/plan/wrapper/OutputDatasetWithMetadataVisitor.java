package openlineage.spark.agent.lifecycle.plan.wrapper;

import com.google.common.collect.ImmutableMap;
import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import openlineage.spark.agent.client.OpenLineageClient;
import openlineage.spark.agent.lifecycle.plan.PlanUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.DataWritingCommand;
import org.apache.spark.sql.execution.command.RunnableCommand;
import org.apache.spark.sql.execution.metric.SQLMetric;
import scala.PartialFunction;
import scala.collection.Map;
import scala.runtime.AbstractPartialFunction;

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
    OpenLineage ol = new OpenLineage(URI.create(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI));

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
            dataset ->
                ol.newOutputDatasetBuilder()
                    .name(dataset.getName())
                    .facets(dataset.getFacets())
                    .namespace(dataset.getNamespace())
                    .outputFacets(
                        new OpenLineage.OutputDatasetOutputFacetsBuilder()
                            .outputStatistics(outputStats)
                            .build())
                    .build())
        .collect(Collectors.toList());
  }
}
