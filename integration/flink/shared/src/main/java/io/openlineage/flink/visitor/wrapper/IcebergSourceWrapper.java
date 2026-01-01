/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;

public class IcebergSourceWrapper<T> {

  private final T source;
  private Class sourceClass;

  public IcebergSourceWrapper(T source, Class sourceClass) {
    this.source = source;
    this.sourceClass = sourceClass;
  }

  public static <T> IcebergSourceWrapper of(T source, Class sourceClass) {
    return new IcebergSourceWrapper(source, sourceClass);
  }

  public Table getTable() {
    return WrapperUtils.<TableLoader>getFieldValue(sourceClass, source, "tableLoader")
        .map(
            loader -> {
              loader.open();
              return loader.loadTable();
            })
        .get();
  }
}
