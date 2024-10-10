/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.util;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.lineage.LineageDataset;

@Slf4j
public class TableLineageDatasetImplUtil {
  private static String TABLE_LINEAGE_CLASS_NAME =
      "org.apache.flink.table.planner.lineage.TableLineageDatasetImpl";
  private static Optional<Class> tableLineageClass = null;

  public static boolean isOnClasspath() {
    if (tableLineageClass == null) {
      try {
        // it's important to use the same classloader as class has
        tableLineageClass =
            Optional.ofNullable(
                TableLineageDatasetImplUtil.class
                    .getClassLoader()
                    .loadClass(TABLE_LINEAGE_CLASS_NAME));
      } catch (ClassNotFoundException e) {
        log.debug("Class {} not present on a classpath", TABLE_LINEAGE_CLASS_NAME);
      }
    }

    return tableLineageClass.isPresent();
  }

  public static boolean isAssignableFrom(LineageDataset dataset) {
    if (!isOnClasspath()) {
      return false;
    }

    return dataset.getClass().isAssignableFrom(tableLineageClass.get());
  }
}
