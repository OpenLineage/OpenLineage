/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;

@Slf4j
public class SparkSessionUtils {

  public static Optional<SparkSession> activeSession() {
    try {
      return Optional.of(SparkSession.active());
    } catch (Exception e) {
      // need to catch exception so that org.apache.spark.SparkException for Spark 4.0 is caught
      // which is not thrown for other Spark versions
      log.debug("Cannot obtain active spark session", e);
      return Optional.empty();
    }
  }

  /**
   * Resolves a catalog by name from the session's catalog manager.
   *
   * <p>Spark 4.2 turned {@code CatalogManager} from a class into an interface, so code compiled
   * against earlier Spark versions emits {@code invokevirtual} and fails at runtime with {@link
   * IncompatibleClassChangeError}. Calling {@code catalog} reflectively keeps this binary
   * compatible with both shapes of the API.
   */
  public static Optional<CatalogPlugin> catalog(SparkSession session, String catalogName) {
    try {
      Object catalogManager = session.sessionState().catalogManager();
      return Optional.ofNullable(
          (CatalogPlugin) MethodUtils.invokeMethod(catalogManager, "catalog", catalogName));
    } catch (Exception e) {
      log.debug("Cannot obtain catalog {} from the session catalog manager", catalogName, e);
      return Optional.empty();
    }
  }
}
