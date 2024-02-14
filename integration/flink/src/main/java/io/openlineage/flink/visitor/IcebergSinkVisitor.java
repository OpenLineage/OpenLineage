/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifierUtils;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.IcebergUtils;
import io.openlineage.flink.visitor.wrapper.IcebergSinkWrapper;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.iceberg.Table;

@Slf4j
public class IcebergSinkVisitor extends Visitor<OpenLineage.OutputDataset> {
  private static final String ICEBERG_FILES_COMMITTER =
      "org.apache.iceberg.flink.sink.IcebergFilesCommitter";

  public IcebergSinkVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object sink) {
    return sink instanceof OneInputTransformation
        && ICEBERG_FILES_COMMITTER.equals(
            ((OneInputTransformation) sink)
                .getOperatorFactory()
                .getStreamOperatorClass(ClassLoader.getSystemClassLoader())
                .getCanonicalName());
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(Object icebergSink) {
    IcebergSinkWrapper sinkWrapper =
        IcebergSinkWrapper.of(((OneInputTransformation) icebergSink).getOperator());
    return sinkWrapper
        .getTable()
        .map(table -> getDataset(context, table))
        .map(dataset -> Collections.singletonList(dataset))
        .orElse(Collections.emptyList());
  }

  private OpenLineage.OutputDataset getDataset(OpenLineageContext context, Table table) {
    OpenLineage openLineage = context.getOpenLineage();
    DatasetIdentifier datasetIdentifier =
        DatasetIdentifierUtils.fromURI(URI.create(table.location()));
    return openLineage
        .newOutputDatasetBuilder()
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
