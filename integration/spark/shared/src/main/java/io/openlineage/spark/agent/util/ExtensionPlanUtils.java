/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.extension.scala.v1.OpenLineageExtensionContext;

public class ExtensionPlanUtils {

  public static OpenLineageExtensionContext context(OpenLineageContext context) {
    return new OpenLineageExtensionContext() {

      @Override
      public OpenLineage openLineage() {
        return context.getOpenLineage();
      }

      @Override
      public String eventType() {
        return "RUNNING"; // TODO: this should be passed as an argument
      }
    };
  }
}
