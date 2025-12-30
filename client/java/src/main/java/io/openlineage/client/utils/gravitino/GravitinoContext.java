/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.utils.gravitino;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * ThreadLocal context for sharing Gravitino configuration across modules. This allows the Spark
 * integration layer to provide Gravitino context to the client layer without modifying method
 * signatures.
 */
@Slf4j
public class GravitinoContext {
  private static final ThreadLocal<GravitinoInfo> CONTEXT = new ThreadLocal<>();

  /**
   * Sets the Gravitino context for the current thread.
   *
   * @param info the Gravitino configuration info
   */
  public static void setContext(GravitinoInfo info) {
    CONTEXT.set(info);
    log.debug(
        "Set Gravitino context for thread: metalake={}, uri={}",
        info.getMetalake().orElse("not set"),
        info.getUri().orElse("not set"));
  }

  /**
   * Gets the Gravitino context for the current thread.
   *
   * @return Optional containing the Gravitino context, or empty if not set
   */
  public static Optional<GravitinoInfo> getContext() {
    GravitinoInfo info = CONTEXT.get();
    if (info != null) {
      log.debug(
          "Retrieved Gravitino context from thread: metalake={}, uri={}",
          info.getMetalake().orElse("not set"),
          info.getUri().orElse("not set"));
      return Optional.of(info);
    } else {
      log.debug("No Gravitino context found in current thread");
      return Optional.empty();
    }
  }

  /**
   * Clears the Gravitino context for the current thread. This should be called to prevent memory
   * leaks, especially in long-running applications or thread pools.
   */
  public static void clearContext() {
    CONTEXT.remove();
    log.debug("Cleared Gravitino context for current thread");
  }

  /**
   * Checks if Gravitino context is available for the current thread.
   *
   * @return true if context is available, false otherwise
   */
  public static boolean hasContext() {
    return CONTEXT.get() != null;
  }
}
