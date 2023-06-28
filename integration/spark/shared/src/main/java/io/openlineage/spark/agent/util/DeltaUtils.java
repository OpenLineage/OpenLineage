/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.util;

public class DeltaUtils {

  public static boolean hasMergeIntoClasses() {
    try {
      DeltaUtils.class
          .getClassLoader()
          .loadClass("org.apache.spark.sql.delta.commands.MergeIntoCommand");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }
}
