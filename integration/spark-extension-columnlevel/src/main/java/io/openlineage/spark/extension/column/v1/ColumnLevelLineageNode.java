/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.extension.column.v1;

import java.util.List;

public interface ColumnLevelLineageNode {
  List<DatasetFieldLineage> getColumnLevelLineageInputs(String sparkListenerEventName);

  List<DatasetFieldLineage> getColumnLevelLineageOutputs(String sparkListenerEventName);

  List<ExpressionDependency> getColumnLevelLineageDependencies(String sparkListenerEventName);
}
