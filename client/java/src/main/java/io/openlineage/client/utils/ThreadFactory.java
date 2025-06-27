/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A ThreadFactory implementation that creates named threads for use in ThreadPoolExecutor. This
 * factory creates threads with a specified name prefix followed by a sequential number.
 */
public class ThreadFactory implements java.util.concurrent.ThreadFactory {
  private final AtomicInteger threadNumber = new AtomicInteger(1);
  private final String namePrefix;
  private final ThreadGroup group;
  private final boolean daemon;

  /**
   * Creates a ThreadFactory with the specified name prefix. Threads will be named as
   * "{namePrefix}-{number}".
   *
   * @param namePrefix the prefix for thread names
   */
  public ThreadFactory(String namePrefix) {
    this(namePrefix, false);
  }

  /**
   * Creates a ThreadFactory with the specified name prefix and daemon flag. Threads will be named
   * as "{namePrefix}-{number}".
   *
   * @param namePrefix the prefix for thread names
   * @param daemon whether threads should be daemon threads
   */
  public ThreadFactory(String namePrefix, boolean daemon) {
    this.group = Thread.currentThread().getThreadGroup();
    this.namePrefix = namePrefix + "-";
    this.daemon = daemon;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
    t.setDaemon(daemon);
    if (t.getPriority() != Thread.NORM_PRIORITY) {
      t.setPriority(Thread.NORM_PRIORITY);
    }
    return t;
  }
}
