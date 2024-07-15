/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import org.apache.spark.sql.SparkSession;

public class SparkVersionUtils {
  public static boolean isSpark3OrHigher() {
    return SparkSessionUtils.activeSession()
        .map(SparkSession::version)
        .filter(v -> "3".compareTo(v) < 0)
        .isPresent();
  }
}
