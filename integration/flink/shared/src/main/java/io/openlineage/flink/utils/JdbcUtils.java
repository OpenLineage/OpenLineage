/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

public class JdbcUtils {

  public static boolean hasClasses() {
    try {
      JdbcUtils.class
          .getClassLoader()
          .loadClass("org.apache.flink.connector.jdbc.internal.JdbcOutputFormat");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }
}
