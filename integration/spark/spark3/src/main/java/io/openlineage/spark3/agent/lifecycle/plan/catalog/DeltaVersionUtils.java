/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;

/**
 * Resolves the Delta snapshot version used as the OpenLineage dataset version. Shared by the
 * catalog handlers that surface Delta tables: {@link DeltaHandler} and the open source Unity
 * Catalog handler in the {@code spark40} module.
 */
@Slf4j
public final class DeltaVersionUtils {

  private DeltaVersionUtils() {}

  /**
   * Returns the Delta snapshot version of the given table, or empty when the table is not a {@link
   * DeltaTableV2} or its snapshot cannot be retrieved.
   */
  public static Optional<String> getDatasetVersion(Table table) {
    if (table instanceof DeltaTableV2) {
      return getDeltaTableSnapshot((DeltaTableV2) table)
          .map(snapshot -> Long.toString(snapshot.version()));
    }
    return Optional.empty();
  }

  /**
   * Versions of Delta differ in implementation of {@link DeltaTableV2} class. This method retrieves
   * the table snapshot regardless of the Delta version. Previously `snapshot` method was available
   * in Scala class for Delta < 3. Recent changes in Delta 3+ changed the naming of this function to
   * be 'initialSnapshot'. The developers wanted to indicate with this rename that the snapshot we
   * are getting, is a lazy value that returns the initial version you read. If the table has been
   * changed in the meantime, you will not get the latest snapshot but that initial version you
   * read.
   *
   * @return Optional SnapShot of the deltaTable.
   */
  private static Optional<Snapshot> getDeltaTableSnapshot(DeltaTableV2 deltaTable) {
    if (MethodUtils.getAccessibleMethod(deltaTable.getClass(), "snapshot") != null) {
      try {
        return Optional.of((Snapshot) MethodUtils.invokeMethod(deltaTable, "snapshot"));
      } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
        log.error("Could not invoke method", e);
      }
    } else if (MethodUtils.getAccessibleMethod(deltaTable.getClass(), "initialSnapshot") != null) {
      try {
        return Optional.of((Snapshot) MethodUtils.invokeMethod(deltaTable, "initialSnapshot"));
      } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
        log.error("Could not invoke method", e);
      }
    }
    return Optional.empty();
  }
}
