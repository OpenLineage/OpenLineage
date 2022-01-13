package io.openlineage.spark3.agent.facets.builder;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.Optional;
import java.util.function.BiConsumer;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.V2WriteCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;

/**
 * {@link CustomFacetBuilder} that generates a {@link OpenLineage.DatasetVersionDatasetFacet} if a
 * {@link org.apache.spark.sql.execution.QueryExecution} is present and underlying system (ex.
 * Delta, Iceberg) supports it.
 *
 * <p>To make sure we're getting right version, we need to get the output dataset's version after
 * we've written it. There's no point in gettting this on START event - the version does not yet
 * exist.
 */
@Slf4j
@AllArgsConstructor
public class DatasetVersionOutputDatasetFacetBuilder
    extends CustomFacetBuilder<SparkListenerSQLExecutionEnd, OpenLineage.DatasetFacet> {

  @NonNull protected final OpenLineageContext context;

  @Override
  public boolean isDefinedAt(Object x) {
    if (!context.getQueryExecution().isPresent()) {
      return false;
    }
    LogicalPlan plan = context.getQueryExecution().get().optimizedPlan();
    return (x instanceof SparkListenerSQLExecutionEnd)
        && (plan instanceof V2WriteCommand)
        && ((((V2WriteCommand) plan).table() instanceof DataSourceV2Relation)
            || (((V2WriteCommand) plan).table() instanceof LogicalRelation));
  }

  @Override
  protected void build(
      SparkListenerSQLExecutionEnd event,
      BiConsumer<String, ? super OpenLineage.DatasetFacet> consumer) {
    LogicalPlan logicalPlan = context.getQueryExecution().get().optimizedPlan();

    Optional<DataSourceV2Relation> maybeTable = PlanUtils3.getDataSourceV2Relation(logicalPlan);
    maybeTable
        .flatMap(DatasetVersionDatasetFacetUtils::extractVersionFromDataSourceV2Relation)
        .ifPresent(
            version ->
                consumer.accept(
                    "datasetVersion",
                    context.getOpenLineage().newDatasetVersionDatasetFacet(version)));

    if (logicalPlan instanceof LogicalRelation) {
      DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(
              (LogicalRelation) logicalPlan)
          .ifPresent(
              version ->
                  consumer.accept(
                      "datasetVersion",
                      context.getOpenLineage().newDatasetVersionDatasetFacet(version)));
    }
  }
}
