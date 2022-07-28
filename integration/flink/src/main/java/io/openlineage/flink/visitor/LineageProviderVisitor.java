/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.DatasetFactory;
import io.openlineage.flink.api.LineageProvider;
import io.openlineage.flink.api.OpenLineageContext;
import java.util.List;
import lombok.NonNull;

/**
 * This class extracts lineage from manually annotated sources and sinks via LineageProvider class.
 */
public class LineageProviderVisitor<D extends OpenLineage.Dataset> extends Visitor<D> {

  private final DatasetFactory<D> datasetFactory;

  public LineageProviderVisitor(
      @NonNull OpenLineageContext context, @NonNull DatasetFactory<D> datasetFactory) {
    super(context);
    this.datasetFactory = datasetFactory;
  }

  @Override
  public boolean isDefinedAt(Object object) {
    return object instanceof LineageProvider;
  }

  @Override
  public List<D> apply(Object object) {
    LineageProvider<D> provider = (LineageProvider) object;
    return provider.getDatasets(datasetFactory);
  }
}
