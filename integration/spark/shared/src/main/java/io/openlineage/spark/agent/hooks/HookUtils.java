/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.hooks;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.stream.Stream;

public class HookUtils {

  public static void preBuild(
      OpenLineageContext context, OpenLineage.RunEventBuilder runEventBuilder) {
    Stream.of(new DatabricksJobEventBuilderHook(context))
        .forEach(hook -> hook.preBuild(runEventBuilder));
  }
}
