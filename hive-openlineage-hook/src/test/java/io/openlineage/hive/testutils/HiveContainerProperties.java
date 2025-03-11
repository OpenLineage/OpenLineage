/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.testutils;

import java.nio.file.Path;
import java.nio.file.Paths;

final class HiveContainerProperties {
  private HiveContainerProperties() {}

  public static final Path HOST_BUILD_DIR =
      Paths.get(System.getProperty("build.dir")).toAbsolutePath();

  public static final String HOOK_JAR = System.getProperty(".hive.hook.jar");

  public static final Path RESOURCES_DIR =
      Paths.get(System.getProperty("resources.dir")).toAbsolutePath();
}
