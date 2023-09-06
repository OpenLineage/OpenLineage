/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;

@Slf4j
public class IcebergSinkWrapper {

  /** IcebergFilesCommitter */
  private final Object icebergFilesCommitter;

  private IcebergSinkWrapper(Object icebergFilesCommitter) {
    this.icebergFilesCommitter = icebergFilesCommitter;
  }

  public static IcebergSinkWrapper of(Object icebergFilesCommitter) {
    return new IcebergSinkWrapper(icebergFilesCommitter);
  }

  public Optional<Table> getTable() {
    Class icebergFilesCommiterClass = null;
    try {
      icebergFilesCommiterClass =
          Class.forName("org.apache.iceberg.flink.sink.IcebergFilesCommitter");
      return WrapperUtils.<TableLoader>getFieldValue(
              icebergFilesCommiterClass, icebergFilesCommitter, "tableLoader")
          .map(TableLoader::loadTable);
    } catch (ClassNotFoundException e) {
      log.warn("Failed extracting table from IcebergFilesCommitter", e);
    }

    return Optional.empty();
  }
}
