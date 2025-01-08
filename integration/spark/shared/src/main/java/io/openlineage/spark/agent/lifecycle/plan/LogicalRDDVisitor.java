/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.Rdds;
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

/**
 * {@link LogicalPlan} visitor that attempts to extract {@link Path}s from a {@link HadoopRDD}
 * wrapped in a {@link LogicalRDD}.The logic is mostly the same as the {@link
 * org.apache.spark.sql.execution.datasources.HadoopFsRelation}, but works with {@link RDD}s that
 * are converted to {@link org.apache.spark.sql.Dataset}s.
 */
@Slf4j
public class LogicalRDDVisitor<D extends OpenLineage.Dataset>
    extends AbstractRDDNodeVisitor<LogicalRDD, D> {

  Set<RDD<?>> flattenedRdds;

  public LogicalRDDVisitor(OpenLineageContext context, DatasetFactory<D> datasetFactory) {
    super(context, datasetFactory);
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    if (x instanceof LogicalRDD) {
      flattenedRdds = Rdds.flattenRDDs(((LogicalRDD) x).rdd(), new HashSet<>());
      return !Rdds.findFileLikeRdds(flattenedRdds).isEmpty()
          && !SqlExecutionRDDVisitor.containsSqlExecution(flattenedRdds);
    } else {
      return false;
    }
  }

  @Override
  public List<D> apply(LogicalPlan x) {
    LogicalRDD logicalRdd = (LogicalRDD) x;
    List<RDD<?>> fileRdds = Rdds.findFileLikeRdds(flattenedRdds);
    flattenedRdds.clear();
    return findInputDatasets(fileRdds, logicalRdd.schema());
  }
}
