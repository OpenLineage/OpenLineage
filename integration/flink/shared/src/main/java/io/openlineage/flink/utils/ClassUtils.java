/*
/* Copyright 2018-2026 contributors to the OpenLineage project
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

  /**
   * Checks if Flink 2.x-specific classes are present on the classpath. Uses
   * JobStatusChangedListenerFactory as it is a Flink 2.x-only class that is not backported through
   * connectors (unlike LineageGraph which can appear in Flink 1.x with modern connectors).
   */
  public static boolean hasFlink2Classes() {
    try {
      ClassUtils.class
          .getClassLoader()
          .loadClass("org.apache.flink.core.execution.JobStatusChangedListenerFactory");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  public static boolean hasAvroClasses() {
    try {
      ClassUtils.class.getClassLoader().loadClass("org.apache.avro.Schema");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }
}
