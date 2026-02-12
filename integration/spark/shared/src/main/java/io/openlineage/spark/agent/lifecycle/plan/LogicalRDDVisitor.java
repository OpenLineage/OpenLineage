/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.Rdds;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.types.StructType;

/**
 * {@link LogicalPlan} visitor that attempts to extract {@link Path}s from a {@link HadoopRDD}
 * wrapped in a {@link LogicalRDD}.The logic is mostly the same as the {@link
 * org.apache.spark.sql.execution.datasources.HadoopFsRelation}, but works with {@link RDD}s that
 * are converted to {@link org.apache.spark.sql.Dataset}s.
 */
@Slf4j
public class LogicalRDDVisitor<D extends OpenLineage.Dataset>
    extends AbstractRDDNodeVisitor<LogicalRDD, D> {

  public LogicalRDDVisitor(OpenLineageContext context, DatasetFactory<D> datasetFactory) {
    super(context, datasetFactory);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return (x instanceof LogicalRDD);
  }

  @Override
  public List<D> apply(LogicalPlan x) {
    Set<RDD<?>> flattenedRdds = Rdds.flattenRDDs(((LogicalRDD) x).rdd(), new HashSet<>());
    List<RDD<?>> fileLikeRdds = Rdds.findFileLikeRdds(flattenedRdds);
    return findInputDatasets(fileLikeRdds, resolveSchema(fileLikeRdds));
  }

  private static StructType resolveSchema(List<RDD<?>> rdds) {
    //  Schema from LogicalRDD::schema is unreliable, because it does not account for
    // transformations that may have been applied to the RDD. It represents the final state of the
    // RDD, not how the data source looks like, so it cannot be used here.
    //  Instead, we should resolve schema from underlying RDDs, but it is possible only in few
    // cases, for example from DataSourceRDD when reading from Iceberg Table.
    return PlanUtils.findSchema(rdds).orElse(null);
  }
}
