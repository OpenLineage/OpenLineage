/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

public final class ScalaVersionUtil {
  private ScalaVersionUtil() {}

  public static final String SCALA_VERSION = scala.util.Properties.versionNumberString();

  public static boolean isScala213() {
    return SCALA_VERSION.startsWith("2.13");
  }
}
