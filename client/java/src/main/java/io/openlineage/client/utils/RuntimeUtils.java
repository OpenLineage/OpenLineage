/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.utils;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

/**
 * Class used to access static Runtime properties. Useful for testing as it can be mocked with
 * mockStatic.
 */
public class RuntimeUtils {
  public static long freeMemory() {
    return Runtime.getRuntime().freeMemory();
  }

  public static long totalMemory() {
    return Runtime.getRuntime().totalMemory();
  }

  public static long maxMemory() {
    return Runtime.getRuntime().maxMemory();
  }

  public static double getMemoryFractionUsage() {
    return (100.0 * ((double) totalMemory())) / ((double) maxMemory());
  }

  public static List<GarbageCollectorMXBean> getGarbageCollectorMXBeans() {
    return ManagementFactory.getGarbageCollectorMXBeans();
  }
}
