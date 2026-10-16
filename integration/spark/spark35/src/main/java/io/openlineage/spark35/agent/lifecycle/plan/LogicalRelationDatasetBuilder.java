/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark35.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;

/**
 * Spark 3.5 builder specialization. Hudi and dataset-version behavior is inherited from the Spark 3
 * implementation to keep a single LogicalRelationDatasetBuilder code path across Spark 3.x.
 */
public class LogicalRelationDatasetBuilder<D extends OpenLineage.Dataset>
    extends io.openlineage.spark3.agent.lifecycle.plan.LogicalRelationDatasetBuilder<D> {

  public LogicalRelationDatasetBuilder(
      OpenLineageContext context, DatasetFactory datasetFactory, boolean searchDependencies) {
    super(context, datasetFactory, searchDependencies);
  }
}
