/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.micrometer.core.instrument.MeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.utils.UUIDUtils;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
  final QueryExecution queryExecution;

  public Optional<QueryExecution> getQueryExecution() {
    return Optional.ofNullable(queryExecution);
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

  /**
   * Job nurn is collected during the Spark runs, and stored for creating a custom facet within
   * the Run facets. It should help us to enhance the events further in the lineage pipeline.
   */
  @Getter @Setter String jobNurn;

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
}
