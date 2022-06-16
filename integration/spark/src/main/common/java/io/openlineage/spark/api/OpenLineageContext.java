/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Tolerate;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import scala.PartialFunction;

/**
 * Context holder with references to several required objects during construction of an OpenLineage
 * {@link io.openlineage.client.OpenLineage.RunEvent}. An {@link OpenLineageContext} should be
 * created once for every detected Spark execution - for a Spark SQL job, that will be for each
 * {@link QueryExecution}, whereas for each Spark RDD job, that will be once for each Spark {@link
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
@Value
@Builder
public class OpenLineageContext {
  UUID runUuid = UUID.randomUUID();

  /**
   * Optional {@link SparkSession} instance when an application is using a Spark SQL configuration
   */
  @Default @NonNull Optional<SparkSession> sparkSession = Optional.empty();

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
  @Default @NonNull
  List<PartialFunction<LogicalPlan, List<InputDataset>>> inputDatasetQueryPlanVisitors =
      new ArrayList<>();

  @Default @NonNull
  List<PartialFunction<Object, Collection<InputDataset>>> inputDatasetBuilders = new ArrayList<>();

  /**
   * A non-null, but potentially empty, list of {@link LogicalPlan} visitors that can extract {@link
   * OutputDataset}s from plan nodes. Useful for delegating from general output visitors to more
   * specific ones.
   */
  @Default @NonNull
  List<PartialFunction<LogicalPlan, List<OutputDataset>>> outputDatasetQueryPlanVisitors =
      new ArrayList<>();

  @Default @NonNull
  List<PartialFunction<Object, Collection<OutputDataset>>> outputDatasetBuilders =
      new ArrayList<>();

  /** Optional {@link QueryExecution} for runs that are Spark SQL queries. */
  @Default @NonNull Optional<QueryExecution> queryExecution = Optional.empty();

  /**
   * Override the default Builder class to take an unwrapped {@link QueryExecution} argument, rather
   * than forcing the caller to wrap the {@link QueryExecution} in an {@link Optional}. The Spark
   * APIs don't return an optional {@link QueryExecution}, so the caller either has an instance or
   * they don't. This is contrasted with the {@link SparkSession}, where the caller generally
   * obtains an optional instance via {@link SparkSession#getActiveSession()}, so will generally
   * prefer to pass in an optional value without first checking to see if #getActiveSession returned
   * a present value.
   */
  public static class OpenLineageContextBuilder {

    // allow this method and the generated method, since the signatures differ
    @Tolerate
    public OpenLineageContextBuilder queryExecution(QueryExecution queryExecution) {
      return queryExecution(Optional.of(queryExecution));
    }
  }
}
