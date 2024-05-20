/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.extension.scala.v1.OpenLineageExtensionContext;
import org.apache.spark.scheduler.SparkListenerEvent;

public class ExtensionPlanUtils {

  public static OpenLineageExtensionContext context(
      SparkListenerEvent event, OpenLineageContext context) {
    return new OpenLineageExtensionContext() {

      @Override
      public OpenLineage openLineage() {
        return context.getOpenLineage();
      }

      @Override
      public String sparkListenerEventName() {
        return event.getClass().getName();
      }
    };
  }

  public static io.openlineage.spark.extension.v1.OpenLineageExtensionContext javaContext(
      SparkListenerEvent event, OpenLineageContext context) {
    return new io.openlineage.spark.extension.v1.OpenLineageExtensionContext() {

      @Override
      public OpenLineage getOpenLineage() {
        return context.getOpenLineage();
      }

      @Override
      public String getSparkListenerEventName() {
        return event.getClass().getName();
      }
    };
  }
}
