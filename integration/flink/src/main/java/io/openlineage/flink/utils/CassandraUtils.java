/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import com.datastax.driver.mapping.annotations.Table;
import java.lang.annotation.Annotation;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CassandraUtils {

  public static boolean hasClasses() {
    try {
      CassandraUtils.class
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

  public static Optional<Table> extractTableAnnotation(Class pojo) {
    Annotation[] annotations = pojo.getAnnotations();
    for (Annotation annotation : annotations) {
      if (annotation instanceof Table) {
        return Optional.of((Table) annotation);
      }
    }

    return Optional.empty();
  }
}
