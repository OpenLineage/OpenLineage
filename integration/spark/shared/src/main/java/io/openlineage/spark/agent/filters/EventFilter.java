/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import org.apache.spark.scheduler.SparkListenerEvent;

public interface EventFilter {

  default boolean isDisabled(SparkListenerEvent event) {
    return false;
  }
}
