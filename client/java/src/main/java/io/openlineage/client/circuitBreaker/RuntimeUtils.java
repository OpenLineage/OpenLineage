/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.circuitBreaker;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

/**
 * Class used to access static Runtime properties. Useful for testing as it can be mocked with
 * mockStatic.
 */
class RuntimeUtils {
  static long freeMemory() {
    return Runtime.getRuntime().freeMemory();
  }

  static long totalMemory() {
    return Runtime.getRuntime().totalMemory();
  }

  static long maxMemory() {
    return Runtime.getRuntime().maxMemory();
  }

  static List<GarbageCollectorMXBean> getGarbageCollectorMXBeans() {
    return ManagementFactory.getGarbageCollectorMXBeans();
  }
}
