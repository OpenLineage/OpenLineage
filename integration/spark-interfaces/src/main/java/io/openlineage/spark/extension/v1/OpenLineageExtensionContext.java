/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.extension.v1;

import io.openlineage.client.OpenLineage;

public interface OpenLineageExtensionContext {
  OpenLineage getOpenLineage();

  /**
   * Class name of SparkListenerEvent that triggered OpenLineageSparkListener. The value is string
   * as we want to keep it loosely coupled from Spark code classes. This can be a name of any class
   * extending SparkListenerEvent. In most cases this will be: SparkListenerSQLExecutionStart,
   * SparkListenerSQLExecutionEnd, SparkListenerJobStart, SparkListenerJobEnd. The value may be
   * helpful to understand context which triggered OpenLineage event being created.
   *
   * @return class name of SparkListenerEvent
   */
  String getSparkListenerEventName();
}
