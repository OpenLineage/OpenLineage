/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.util;

public class DeltaUtils {

  public static boolean hasMergeIntoCommandClass() {
    return ReflectionUtils.hasClass("org.apache.spark.sql.delta.commands.MergeIntoCommand");
  }
}
