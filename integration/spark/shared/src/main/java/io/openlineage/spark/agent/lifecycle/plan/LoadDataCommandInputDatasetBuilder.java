/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.LoadDataCommand;

/**
 * Extracts the source location read by a {@link LoadDataCommand} as an input {@link
 * OpenLineage.Dataset}. The target table is emitted as the output by {@link
 * LoadDataCommandDatasetBuilder}.
 */
@Slf4j
public class LoadDataCommandInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<LoadDataCommand> {

  public LoadDataCommandInputDatasetBuilder(OpenLineageContext context) {
    super(context, true);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof LoadDataCommand;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(LoadDataCommand command) {
    String path = command.path();
    if (StringUtils.isBlank(path)) {
      return Collections.emptyList();
    }

    try {
      return Collections.singletonList(
          inputDataset().sparkDatasetBuilder().dataset(PathUtils.fromPath(new Path(path))).build());
    } catch (Exception e) {
      // A LOAD DATA path may be a glob or an otherwise unparsable location
      log.warn("Could not build an input dataset from LOAD DATA path {}", path, e);
      return Collections.emptyList();
    }
  }
}
