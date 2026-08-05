/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.LoadDataCommand;

/**
 * {@link LogicalPlan} visitor that matches a {@link LoadDataCommand} and extracts the source
 * location being read as an input {@link OpenLineage.Dataset}. The target table is emitted as the
 * output by {@link LoadDataCommandVisitor}.
 */
@Slf4j
public class LoadDataCommandInputVisitor
    extends QueryPlanVisitor<LoadDataCommand, OpenLineage.InputDataset> {

  public LoadDataCommandInputVisitor(OpenLineageContext context) {
    super(context);
  }

  @Override
  public List<OpenLineage.InputDataset> apply(LogicalPlan x) {
    String path = ((LoadDataCommand) x).path();
    if (StringUtils.isBlank(path)) {
      return Collections.emptyList();
    }

    try {
      return Collections.singletonList(
          inputDataset().sparkDatasetBuilder().dataset(PathUtils.fromPath(new Path(path))).build());
    } catch (Exception e) {
      // A LOAD DATA path may be a glob or an otherwise unparseable location
      log.warn("Could not build an input dataset from LOAD DATA path {}", path, e);
      return Collections.emptyList();
    }
  }
}
