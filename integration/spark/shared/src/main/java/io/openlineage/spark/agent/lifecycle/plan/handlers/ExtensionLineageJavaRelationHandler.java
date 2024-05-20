/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.handlers;

import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.ExtensionPlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.extension.v1.LineageRelation;
import java.util.Collections;
import java.util.List;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.datasources.LogicalRelation;

public class ExtensionLineageJavaRelationHandler<D extends Dataset> {
  private final DatasetFactory<D> datasetFactory;
  private final OpenLineageContext context;

  public ExtensionLineageJavaRelationHandler(
      OpenLineageContext context, DatasetFactory<D> datasetFactory) {
    this.datasetFactory = datasetFactory;
    this.context = context;
  }

  public List<D> handleRelation(SparkListenerEvent event, LogicalRelation x) {
    if (!(x.relation() instanceof LineageRelation)) {
      return Collections.emptyList();
    }

    LineageRelation relation = (LineageRelation) x.relation();
    DatasetIdentifier di =
        relation.getLineageDatasetIdentifier(ExtensionPlanUtils.javaContext(event, context));

    if (x.schema() != null) {
      return Collections.singletonList(datasetFactory.getDataset(di, x.schema()));
    } else {
      return Collections.singletonList(
          datasetFactory.getDataset(di, context.getOpenLineage().newDatasetFacetsBuilder()));
    }
  }
}
