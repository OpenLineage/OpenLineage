/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.util;

import java.util.Arrays;

public class DeltaUtils {

  public static boolean hasMergeIntoCommandClass() {
    return hasClass("org.apache.spark.sql.delta.commands.MergeIntoCommand");
  }

  public static boolean hasClasses(String... classes) {
    return Arrays.stream(classes).allMatch(DeltaUtils::hasClass);
  }

  private static boolean hasClass(String aClass) {
    try {
      DeltaUtils.class.getClassLoader().loadClass(aClass);
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }
}
