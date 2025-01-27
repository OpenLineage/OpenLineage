/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.micrometer.core.instrument.MeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.utils.UUIDUtils;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.spark.SparkContext;
import org.apache.spark.package$;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import scala.PartialFunction;

/**
 * Context holder with references to several required objects during construction of an OpenLineage
 * {@link OpenLineage.RunEvent}. An {@link OpenLineageContext} should be created once for every
 * detected Spark execution - for a Spark SQL job, that will be for each {@link QueryExecution},
 * whereas for each Spark RDD job, that will be once for each Spark {@link
 * org.apache.spark.scheduler.ActiveJob}.
 *
 * <p>It should be assumed that the lists of input and output {@link QueryPlanVisitor} are mutable
 * lists. As {@link QueryPlanVisitor}s require a reference to the {@link OpenLineageContext}, the
 * lists will always be added to after the {@link QueryPlanVisitor}s are constructed. Thus, copies
 * should never be made of the lists, as it should be assumed such copies will be incomplete.
 *
 * <p>This API is evolving and may change in future releases
 *
 * @apiNote This interface is evolving and may change in future releases
 */
@Builder
public class OpenLineageContext {
  // filled up only for SparkListenerApplication{Start,End} events
  @Setter @Getter UUID applicationUuid;

  // filled up for SparkListener non-application events
  @Default @NonNull @Getter final UUID runUuid = UUIDUtils.generateNewUUID();

  /** {@link SparkSession} instance when an application is using a Spark SQL configuration */
  final SparkSession sparkSession;

  public Optional<SparkSession> getSparkSession() {
    return Optional.ofNullable(sparkSession);
  }

  /** The {@link SparkContext} running for the application we're reporting run data for */
  final SparkContext sparkContext;

  public Optional<SparkContext> getSparkContext() {
    if (sparkContext != null) {
      return Optional.of(sparkContext);
    }
    return getSparkSession().map(session -> session.sparkContext());
  }

  /** The list of custom environment variables to be captured */
  @Default @NonNull @Getter final List<String> customEnvironmentVariables = Collections.emptyList();

  /**
   * A non-null, preconfigured {@link OpenLineage} client instance for constructing OpenLineage
   * model objects
   */
  @NonNull @Getter final OpenLineage openLineage;

  /**
   * A non-null, but potentially empty, list of {@link LogicalPlan} visitors that can extract {@link
   * InputDataset}s from plan nodes. Useful for delegating from general input visitors to more
   * specific ones.
   */
  @Default @NonNull @Getter
  final List<PartialFunction<LogicalPlan, List<InputDataset>>> inputDatasetQueryPlanVisitors =
      new ArrayList<>();

  @Default @NonNull @Getter
  final List<PartialFunction<Object, Collection<InputDataset>>> inputDatasetBuilders =
      new ArrayList<>();

  /**
   * A non-null, but potentially empty, list of {@link LogicalPlan} visitors that can extract {@link
   * OutputDataset}s from plan nodes. Useful for delegating from general output visitors to more
   * specific ones.
   */
  @Default @NonNull @Getter
  final List<PartialFunction<LogicalPlan, List<OutputDataset>>> outputDatasetQueryPlanVisitors =
      new ArrayList<>();

  @Default @NonNull @Getter
  final List<PartialFunction<Object, Collection<OutputDataset>>> outputDatasetBuilders =
      new ArrayList<>();

  /**
   * List of column level lineage visitors to be added dynamically based on Spark version and
   * versions of the 3rd party libraries
   */
  @Default @NonNull @Getter
  final List<ColumnLevelLineageVisitor> columnLevelLineageVisitors = new ArrayList<>();

  /** Optional {@link QueryExecution} for runs that are Spark SQL queries. */
  private final QueryExecution queryExecution;

  /**
   * @deprecated Use the direct methods like {@link #getLogicalPlan()}, {@link #getAnalyzedPlan()},
   *     {@link #getOptimizedPlan()}, or {@link #getSparkPlan()} to access the underlying plans.
   *     This method exposes the internal {@link QueryExecution} object, which breaks the Law of
   *     Demeter and is not intended for direct use. The direct methods provide a cleaner API for
   *     accessing plan information and also centralize logic for handling cases where the plan may
   *     need to be augmented.
   * @return an Optional {@link QueryExecution}
   */
  @Deprecated
  public Optional<QueryExecution> getQueryExecution() {
    return Optional.ofNullable(queryExecution);
  }

  /**
   * Checks if a logical plan is available for the current query execution.
   *
   * @return true if a logical plan exists, false otherwise.
   */
  public boolean hasLogicalPlan() {
    return queryExecution != null && queryExecution.logical() != null;
  }

  /**
   * Checks if an analyzed logical plan is available for the current query execution.
   *
   * @return true if an analyzed logical plan exists, false otherwise.
   */
  public boolean hasAnalyzedPlan() {
    return queryExecution != null && queryExecution.analyzed() != null;
  }

  /**
   * Checks if an optimized logical plan is available for the current query execution.
   *
   * @return true if an optimized logical plan exists, false otherwise.
   */
  public boolean hasOptimizedPlan() {
    return queryExecution != null && queryExecution.optimizedPlan() != null;
  }

  /**
   * Returns the logical plan for the current query execution.
   *
   * @return The logical plan.
   * @throws NullPointerException if there is no query execution or the logical plan is not present
   */
  public LogicalPlan getLogicalPlan() {
    return queryExecution.logical();
  }

  /**
   * Returns the analyzed logical plan for the current query execution.
   *
   * @return The analyzed logical plan.
   * @throws NullPointerException if there is no query execution or the analyzed logical plan is not
   *     present
   */
  public LogicalPlan getAnalyzedPlan() {
    return queryExecution.analyzed();
  }

  /**
   * Returns the optimized logical plan for the current query execution.
   *
   * @return The optimized logical plan.
   * @throws NullPointerException if there is no query execution or the optimized logical plan is
   *     not present
   */
  public LogicalPlan getOptimizedPlan() {
    return queryExecution.optimizedPlan();
  }

  /**
   * Returns an Optional containing the logical plan for the current query execution.
   *
   * @return An Optional containing the logical plan, or an empty Optional if not present.
   */
  public Optional<LogicalPlan> getLogicalPlanOptional() {
    return Optional.ofNullable(queryExecution.logical());
  }

  /**
   * Returns an Optional containing the analyzed logical plan for the current query execution.
   *
   * @return An Optional containing the analyzed logical plan, or an empty Optional if not present.
   */
  public Optional<LogicalPlan> getAnalyzedPlanOptional() {
    return Optional.ofNullable(queryExecution.analyzed());
  }

  /**
   * Returns an Optional containing the optimized logical plan for the current query execution.
   *
   * @return An Optional containing the optimized logical plan, or an empty Optional if not present.
   */
  public Optional<LogicalPlan> getOptimizedPlanOptional() {
    return Optional.ofNullable(queryExecution.optimizedPlan());
  }

  /**
   * Checks if an executed plan is available for the current query execution.
   *
   * @return true if an executed plan exists, false otherwise.
   */
  public boolean hasExecutedPlan() {
    return queryExecution != null && queryExecution.executedPlan() != null;
  }

  /**
   * Returns the executed plan for the current query execution.
   *
   * @return The executed plan.
   * @throws NullPointerException if there is no query execution or the executed plan is not present
   */
  public SparkPlan getExecutedPlan() {
    return queryExecution.executedPlan();
  }

  /**
   * Checks if a spark plan is available for the current query execution.
   *
   * @return true if a spark plan exists, false otherwise.
   */
  public boolean hasSparkPlan() {
    return queryExecution != null && queryExecution.sparkPlan() != null;
  }

  /**
   * Returns the spark plan for the current query execution.
   *
   * @return The spark plan.
   * @throws NullPointerException if there is no query execution or the spark plan is not present
   */
  public SparkPlan getSparkPlan() {
    return queryExecution.sparkPlan();
  }

  /** Spark version of currently running job */
  public String getSparkVersion() {
    return getSparkContext().map(sc -> sc.version()).orElse(package$.MODULE$.SPARK_VERSION());
  }

  /**
   * Job name is build when the first event of the run is build is created on the top of ready event
   * based on the output dataset being present within an event. It is stored within a context to
   * become consistent over a run progress.
   */
  @Getter @Setter String jobName;

  @Setter Integer activeJobId;

  public Optional<Integer> getActiveJobId() {
    return Optional.ofNullable(activeJobId);
  }

  /**
   * Contains the vendors like Snowflake to separate the dependencies from the app and shared
   * project.
   */
  @Default @NonNull @Getter Vendors vendors = Vendors.empty();

  /** Already set up MeterRegistry to use in ExecutionContext and Visitors */
  @NonNull @Getter final MeterRegistry meterRegistry;

  /**
   * Config container containing config entries from both SparkConf as well as openlineage.yaml
   * file. All the configuration should be read from this object.
   */
  @NonNull @Getter final SparkOpenLineageConfig openLineageConfig;

  /**
   * Helper class that uses reflection to call methods of SparkOpenLineageExtensionVisitor
   * instances. This class acts as a bridge, facilitating communication between openlineage-spark
   * and the extension implementations provided by spark-extension-interfaces.
   */
  @Getter final SparkOpenLineageExtensionVisitorWrapper sparkExtensionVisitorWrapper;

  @Override
  public String toString() {
    return new StringJoiner(", ", OpenLineageContext.class.getSimpleName() + "[", "]")
        .add("applicationUuid=" + applicationUuid)
        .add("runUuid=" + runUuid)
        .add("jobName='" + jobName + "'")
        .toString();
  }
}
