/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.StreamingMonitorFunction;

public class IcebergSourceWrapper {

  private final StreamingMonitorFunction source;

  public IcebergSourceWrapper(StreamingMonitorFunction streamingMonitorFunction) {
    this.source = streamingMonitorFunction;
  }

  public static IcebergSourceWrapper of(StreamingMonitorFunction streamingMonitorFunction) {
    return new IcebergSourceWrapper(streamingMonitorFunction);
  }

  public Table getTable() {
    return WrapperUtils.<TableLoader>getFieldValue(
            StreamingMonitorFunction.class, source, "tableLoader")
        .map(TableLoader::loadTable)
        .get();
  }
}
