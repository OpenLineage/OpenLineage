package marquez.spark.agent.lifecycle.plan;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.List;
import java.util.stream.Collectors;
import marquez.spark.agent.client.LineageEvent.Dataset;
import marquez.spark.agent.facets.OutputStatisticsFacet;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

/**
 * {@link LogicalPlan} visitor that matches an {@link InsertIntoDataSourceCommand} and extracts the
 * output {@link Dataset} being written.
 */
public class InsertIntoDataSourceVisitor
    extends AbstractPartialFunction<LogicalPlan, List<Dataset>> {
  private final List<PartialFunction<LogicalPlan, List<Dataset>>> datasetProviders;

  public InsertIntoDataSourceVisitor(
      List<PartialFunction<LogicalPlan, List<Dataset>>> datasetProviders) {
    this.datasetProviders = datasetProviders;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof InsertIntoDataSourceCommand;
  }

  @Override
  public List<Dataset> apply(LogicalPlan x) {
    OutputStatisticsFacet outputStats =
        PlanUtils.getOutputStats(((InsertIntoDataSourceCommand) x).metrics());
    return PlanUtils.applyFirst(
            datasetProviders, ((InsertIntoDataSourceCommand) x).logicalRelation())
        .stream()
        // constructed datasets don't include the output stats, so add that facet here
        .peek(
            ds -> {
              Builder<String, Object> facetsMap =
                  ImmutableMap.<String, Object>builder().put("stats", outputStats);
              if (ds.getFacets().getAdditionalFacets() != null) {
                facetsMap.putAll(ds.getFacets().getAdditionalFacets());
              }
              ds.getFacets().setAdditional(facetsMap.build());
            })
        .collect(Collectors.toList());
  }
}
