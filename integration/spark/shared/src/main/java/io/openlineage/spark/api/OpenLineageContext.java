/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.micrometer.core.instrument.MeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
@AllArgsConstructor
@Builder
public class OpenLineageContext {
  @Default @NonNull @Getter final UUID runUuid = UUID.randomUUID();

  /** {@link SparkSession} instance when an application is using a Spark SQL configuration */
  final SparkSession sparkSession;

  public Optional<SparkSession> getSparkSession() {
    return Optional.ofNullable(sparkSession);
  }

  /** The non-null {@link SparkContext} running for the application we're reporting run data for */
  @NonNull @Getter final SparkContext sparkContext;

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
  @Default @NonNull @Getter String sparkVersion = package$.MODULE$.SPARK_VERSION();

  /**
   * Job name is build when the first event of the run is build is created on the top of ready event
   * based on the output dataset being present within an event. It is stored within a context to
   * become consistent over a run progress.
   */
  @Getter @Setter String jobName;

  /**
   * Contains the vendors like Snowflake to separate the dependencies from the app and shared
   * project.
   */
  @Default @NonNull @Getter Vendors vendors = Vendors.empty();

  /** Already set up MeterRegistry to use in ExecutionContext and Visitors */
  @NonNull @Getter final MeterRegistry meterRegistry;
}
