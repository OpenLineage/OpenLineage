/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import lombok.extern.slf4j.Slf4j;

/** Utility class to verify existence of certain classes on classpath. */
@Slf4j
public class ClassUtils {

  public static boolean hasCassandraClasses() {
    try {
      ClassUtils.class
          .getClassLoader()
          .loadClass("org.apache.flink.batch.connectors.cassandra.CassandraInputFormat");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
      log.debug(
          "Can't load class org.apache.flink.batch.connectors.cassandra.CassandraInputFormat");
    }
    return false;
  }

  public static boolean hasJdbcClasses() {
    try {
      ClassUtils.class
          .getClassLoader()
          .loadClass("org.apache.flink.connector.jdbc.internal.JdbcOutputFormat");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  public static boolean hasIcebergClasses() {
    try {
      ClassUtils.class
          .getClassLoader()
          .loadClass("org.apache.iceberg.flink.source.StreamingMonitorFunction");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  public static boolean hasFlink2Classes() {
    try {
      ClassUtils.class
          .getClassLoader()
          .loadClass("org.apache.flink.streaming.api.lineage.LineageGraph");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }
}
