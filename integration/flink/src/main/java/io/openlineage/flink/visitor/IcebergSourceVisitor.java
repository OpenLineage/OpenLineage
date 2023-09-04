/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.DatasetIdentifier;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.IcebergUtils;
import io.openlineage.flink.visitor.wrapper.IcebergSourceWrapper;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.source.StreamingMonitorFunction;

@Slf4j
public class IcebergSourceVisitor extends Visitor<OpenLineage.InputDataset> {
  public IcebergSourceVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object source) {
    return source instanceof StreamingMonitorFunction;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(Object source) {
    IcebergSourceWrapper sourceWrapper = IcebergSourceWrapper.of((StreamingMonitorFunction) source);
    return Collections.singletonList(getDataset(context, sourceWrapper.getTable()));
  }

  private OpenLineage.InputDataset getDataset(OpenLineageContext context, Table table) {
    OpenLineage openLineage = context.getOpenLineage();
    DatasetIdentifier datasetIdentifier = DatasetIdentifier.fromUri(URI.create(table.location()));
    return openLineage
        .newInputDatasetBuilder()
        .name(datasetIdentifier.getName())
        .namespace(datasetIdentifier.getNamespace())
        .facets(
            openLineage
                .newDatasetFacetsBuilder()
                .schema(IcebergUtils.getSchema(context, table))
                .build())
        .build();
  }
}
