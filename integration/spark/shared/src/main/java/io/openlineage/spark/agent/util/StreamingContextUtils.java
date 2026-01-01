/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import org.apache.spark.streaming.StreamingContext$;

/** Utility methods to deal with StreamingContext */
public class StreamingContextUtils {

  public static boolean hasActiveStreamingContext() {
    if (!ReflectionUtils.hasClass("org.apache.spark.streaming.StreamingContext")) {
      return false;
    }
    return StreamingContext$.MODULE$.getActive().isDefined();
  }
}
