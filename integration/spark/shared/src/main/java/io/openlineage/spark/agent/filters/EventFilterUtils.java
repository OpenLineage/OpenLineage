/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import org.apache.spark.scheduler.SparkListenerEvent;

public class EventFilterUtils {

  /**
   * Method that verifies based on OpenLineageContext and SparkListenerEvent if OpenLineage event
   * has to be sent.
   *
   * @param context
   * @param event
   * @return
   */
  public static boolean isDisabled(OpenLineageContext context, SparkListenerEvent event) {
    return Arrays.asList(new DeltaEventFilter(context), new DatabricksEventFilter(context)).stream()
        .filter(filter -> filter.isDisabled(event.getClass().cast(event)))
        .findAny()
        .isPresent();
  }
}
