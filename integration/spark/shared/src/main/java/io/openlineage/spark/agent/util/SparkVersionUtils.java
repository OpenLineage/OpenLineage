/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import org.apache.spark.sql.SparkSession;

public class SparkVersionUtils {
  public static boolean isSpark3() {
    return SparkSession.active().version().startsWith("3");
  }
}
