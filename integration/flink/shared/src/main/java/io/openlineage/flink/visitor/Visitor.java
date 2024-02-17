/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.DatasetFactory;
import io.openlineage.flink.api.OpenLineageContext;
import java.util.List;
import lombok.NonNull;

public abstract class Visitor<D extends OpenLineage.Dataset> {
  @NonNull protected final OpenLineageContext context;

  public Visitor(@NonNull OpenLineageContext context) {
    this.context = context;
  }

  protected DatasetFactory<OpenLineage.OutputDataset> outputDataset() {
    return DatasetFactory.output(context.getOpenLineage());
  }

  protected DatasetFactory<OpenLineage.InputDataset> inputDataset() {
    return DatasetFactory.input(context.getOpenLineage());
  }

  protected OpenLineage.InputDataset createInputDataset(
      OpenLineageContext context, String namespace, String name) {
    OpenLineage openLineage = context.getOpenLineage();
    return openLineage.newInputDatasetBuilder().name(name).namespace(namespace).build();
  }

  protected OpenLineage.OutputDataset createOutputDataset(
      OpenLineageContext context, String namespace, String name) {
    OpenLineage openLineage = context.getOpenLineage();
    return openLineage.newOutputDatasetBuilder().name(name).namespace(namespace).build();
  }

  public abstract boolean isDefinedAt(Object object);

  public abstract List<D> apply(Object object);
}
