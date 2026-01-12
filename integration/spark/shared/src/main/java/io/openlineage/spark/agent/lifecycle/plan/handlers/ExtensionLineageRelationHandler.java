/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.lifecycle.plan.handlers;

import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.datasources.LogicalRelation;

public class ExtensionLineageRelationHandler<D extends Dataset> {
  private final DatasetFactory<D> datasetFactory;
  private final OpenLineageContext context;

  public ExtensionLineageRelationHandler(
      OpenLineageContext context, DatasetFactory<D> datasetFactory) {
    this.datasetFactory = datasetFactory;
    this.context = context;
  }

  public List<D> handleRelation(SparkListenerEvent event, LogicalRelation x) {
    if (!context.getSparkExtensionVisitorWrapper().isDefinedAt(x.relation())) {
      return Collections.emptyList();
    }

    DatasetIdentifier di =
        context
            .getSparkExtensionVisitorWrapper()
            .getLineageDatasetIdentifier(x.relation(), event.getClass().getName());

    if (x.schema() != null) {
      return Collections.singletonList(datasetFactory.getDataset(di, x.schema()));
    } else {
      return Collections.singletonList(
          datasetFactory.getDataset(di, datasetFactory.createCompositeFacetBuilder()));
    }
  }
}
