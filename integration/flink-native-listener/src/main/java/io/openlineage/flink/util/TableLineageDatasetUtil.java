/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.util;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableLineageDatasetUtil {
  private static String TABLE_LINEAGE_CLASS_NAME =
      "org.apache.flink.table.planner.lineage.TableLineageDataset";
  private static Optional<Class> tableLineageClass = null;

  public static boolean isOnClasspath() {
    if (tableLineageClass == null) {
      try {
        // it's important to use the same classloader as class has
        tableLineageClass =
            Optional.ofNullable(
                TableLineageDatasetUtil.class.getClassLoader().loadClass(TABLE_LINEAGE_CLASS_NAME));
      } catch (ClassNotFoundException e) {
        log.debug("Class {} not present on a classpath", TABLE_LINEAGE_CLASS_NAME);
      }
    }

    return tableLineageClass.isPresent();
  }
}
