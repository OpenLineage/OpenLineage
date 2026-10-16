/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoDataSourceDatasetBuilder;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import org.apache.spark.scheduler.SparkListenerEvent;

/**
 * Dataset builders registered for every Spark version. Each {@link DatasetBuilderFactory} appends
 * them after its version specific builders, so builders listed here resolve a plan node or a job
 * name suffix only when no version specific builder does.
 */
final class CommonDatasetBuilders {

  private CommonDatasetBuilders() {}

  static List<AbstractQueryPlanDatasetBuilder<SparkListenerEvent, ?, OpenLineage.OutputDataset>>
      outputs(OpenLineageContext context) {
    return ImmutableList.of(new InsertIntoDataSourceDatasetBuilder(context));
  }
}
