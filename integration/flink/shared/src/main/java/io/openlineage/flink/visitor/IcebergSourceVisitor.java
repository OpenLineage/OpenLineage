/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.FilesystemDatasetUtils;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.IcebergSchemaUtils;
import io.openlineage.flink.visitor.wrapper.IcebergSourceWrapper;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.IcebergTableSource;
import org.apache.iceberg.flink.source.StreamingMonitorFunction;

@Slf4j
public class IcebergSourceVisitor extends Visitor<OpenLineage.InputDataset> {
  public IcebergSourceVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object source) {
    return source instanceof StreamingMonitorFunction
        || source instanceof IcebergSource
        || source instanceof IcebergTableSource;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(Object source) {
    IcebergSourceWrapper sourceWrapper;
    if (source instanceof StreamingMonitorFunction) {
      sourceWrapper = IcebergSourceWrapper.of(source, StreamingMonitorFunction.class);
    } else if (source instanceof IcebergSource) {
      sourceWrapper = IcebergSourceWrapper.of(source, IcebergSource.class);
    } else if (source instanceof IcebergTableSource) {
      sourceWrapper = IcebergSourceWrapper.of(source, IcebergTableSource.class);
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported Iceberg Source type %s", source.getClass().getCanonicalName()));
    }

    return Collections.singletonList(getDataset(context, sourceWrapper.getTable()));
  }

  private OpenLineage.InputDataset getDataset(OpenLineageContext context, Table table) {
    OpenLineage openLineage = context.getOpenLineage();
    DatasetIdentifier datasetIdentifier =
        FilesystemDatasetUtils.fromLocation(URI.create(table.location()));
    return openLineage
        .newInputDatasetBuilder()
        .name(datasetIdentifier.getName())
        .namespace(datasetIdentifier.getNamespace())
        .facets(
            openLineage
                .newDatasetFacetsBuilder()
                .schema(IcebergSchemaUtils.convert(openLineage, table.schema()))
                .build())
        .build();
  }
}
