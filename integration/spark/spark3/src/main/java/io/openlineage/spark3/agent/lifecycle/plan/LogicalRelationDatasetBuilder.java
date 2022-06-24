/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.DatasetVersionDatasetFacetUtils;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.datasources.LogicalRelation;

/**
 * Class extending {@link io.openlineage.spark.agent.lifecycle.plan.LogicalRelationDatasetBuilder}
 * with methods only available for Spark3. It is required to support datasetVersionFacet for delta
 * provider
 */
@Slf4j
public class LogicalRelationDatasetBuilder<D extends OpenLineage.Dataset>
    extends io.openlineage.spark.agent.lifecycle.plan.LogicalRelationDatasetBuilder<D> {

  public LogicalRelationDatasetBuilder(
      OpenLineageContext context, DatasetFactory datasetFactory, boolean searchDependencies) {
    super(context, datasetFactory, searchDependencies);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return true;
  }

  @Override
  protected Optional<String> getDatasetVersion(LogicalRelation x) {
    return DatasetVersionDatasetFacetUtils.extractVersionFromLogicalRelation(x);
  }
}
