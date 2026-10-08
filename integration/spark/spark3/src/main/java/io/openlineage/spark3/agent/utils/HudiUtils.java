/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hadoop.fs.Path;

/**
 * Reflection based helpers for Hudi relations. Hudi classes are not guaranteed to be present on the
 * classpath, so they are accessed by name only.
 */
@Slf4j
public class HudiUtils {
  private static final String HOODIE_BASE_RELATION_CLASS = "org.apache.hudi.HoodieBaseRelation";

  private HudiUtils() {}

  /** Checks whether the relation is a Hudi {@code HoodieBaseRelation} (e.g. MOR or incremental). */
  public static boolean isHudiBaseRelation(Object relation) {
    return isInstanceOf(relation, HOODIE_BASE_RELATION_CLASS);
  }

  public static boolean isInstanceOf(Object instance, String className) {
    try {
      return Class.forName(className).isInstance(instance);
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  /** Returns the meta client of a {@code HoodieBaseRelation}. */
  public static Object metaClient(Object relation) {
    try {
      return MethodUtils.invokeMethod(relation, "metaClient");
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalStateException("Unable to resolve Hudi meta client", e);
    }
  }

  /** Resolves the base path of a {@code HoodieBaseRelation}. */
  public static URI basePath(Object relation) {
    return basePath(relation, metaClient(relation));
  }

  /**
   * Resolves the base path of a {@code HoodieBaseRelation} with an already resolved meta client.
   */
  public static URI basePath(Object relation, Object metaClient) {
    try {
      Path path = (Path) MethodUtils.invokeMethod(relation, "basePath");
      if (path != null) {
        return path.toUri();
      }
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      log.debug("Unable to resolve Hudi relation basePath via reflection", e);
    }

    try {
      Object storagePath = MethodUtils.invokeMethod(metaClient, "getBasePathV2");
      return (URI) MethodUtils.invokeMethod(storagePath, "toUri");
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalStateException("Unable to resolve Hudi base path", e);
    }
  }
}
