/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import io.openlineage.spark.api.OpenLineageContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.spark.scheduler.SparkListenerEvent;

@AllArgsConstructor
public class ColumnLevelLineageContext {

  @NonNull @Getter final SparkListenerEvent event;
  @NonNull @Getter final OpenLineageContext olContext;
  @NonNull @Getter final ColumnLevelLineageBuilder builder;
}
