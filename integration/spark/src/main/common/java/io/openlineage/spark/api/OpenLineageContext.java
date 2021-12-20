package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.Value;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import scala.PartialFunction;

/**
 * Context holder with references to several required objects during construction of an OpenLineage
 * {@link io.openlineage.client.OpenLineage.RunEvent}.
 */
@Value
public class OpenLineageContext {

  /**
   * Optional {@link SparkSession} instance when an application is using a Spark SQL configuration
   */
  @NonNull Optional<SparkSession> sparkSession;

  /** The non-null {@link SparkContext} running for the application we're reporting run data for */
  @NonNull SparkContext sparkContext;

  /**
   * A non-null, preconfigured {@link OpenLineage} client instance for constructing OpenLineage
   * model objects
   */
  @NonNull OpenLineage openLineage;

  /**
   * A non-null, but potentially empty, list of {@link LogicalPlan} visitors that can extract {@link
   * InputDataset}s from plan nodes. Useful for delegating from general input visitors to more
   * specific ones.
   */
  @NonNull List<PartialFunction<LogicalPlan, List<InputDataset>>> inputDatasetQueryPlanVisitors;

  /**
   * A non-null, but potentially empty, list of {@link LogicalPlan} visitors that can extract {@link
   * OutputDataset}s from plan nodes. Useful for delegating from general output visitors to more
   * specific ones.
   */
  @NonNull List<PartialFunction<LogicalPlan, List<OutputDataset>>> outputDatasetQueryPlanVisitors;

  /** Optional {@link QueryExecution} for runs that are Spark SQL queries. */
  @NonNull Optional<QueryExecution> queryExecution;
}
