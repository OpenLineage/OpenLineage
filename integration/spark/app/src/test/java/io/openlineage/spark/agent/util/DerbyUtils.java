/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.nio.file.Paths;
import java.time.Instant;

public class DerbyUtils {

  public static void loadSystemProperty(String testName) {
    String derbyHome =
        Paths.get(System.getProperty("derby.system.home.base"))
            .toAbsolutePath()
            .resolve(testName)
            .resolve(String.valueOf(Instant.now().getEpochSecond()))
            .toString();
    System.setProperty("derby.system.home", derbyHome);
  }

  public static void clearDerbyProperty() {
    System.clearProperty("derby.system.home");
  }
}
