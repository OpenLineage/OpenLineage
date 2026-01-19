/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.flink.visitor.wrapper;

import java.lang.reflect.Method;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.paimon.table.Table;

@Slf4j
public class PaimonTableSinkWrapper {
  private final Object paimonCommitterOperator;

  private PaimonTableSinkWrapper(Object paimonCommitterOperator) {
    this.paimonCommitterOperator = paimonCommitterOperator;
  }

  public static PaimonTableSinkWrapper of(Object paimonCommitterOperator) {
    return new PaimonTableSinkWrapper(paimonCommitterOperator);
  }

  public Optional<Table> getTable() {
    try {
      Optional<Table> paimonTable =
          WrapperUtils.getFieldValue(
              paimonCommitterOperator.getClass(), paimonCommitterOperator, "table");

      return paimonTable;
    } catch (Exception e) {
      log.warn("Failed to load Paimon CommitterOperator class", e);
    }
    return Optional.empty();
  }

  // Note: This need more testing, it can change depending on how Paimon is used 
  public static Optional<String> getFullTableName(Table table) {
    return table.getClass().getMethod("fullName");
  }
}
