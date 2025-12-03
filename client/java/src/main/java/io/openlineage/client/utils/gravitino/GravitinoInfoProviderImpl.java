/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.utils.gravitino;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GravitinoInfoProviderImpl {
  private volatile Optional<GravitinoInfo> gravitinoInfo = Optional.empty();
  private final List<GravitinoInfoProvider> providers =
      Arrays.asList(new SparkGravitinoInfoProvider());

  private static class Holder {
    private static final GravitinoInfoProviderImpl INSTANCE = new GravitinoInfoProviderImpl();
  }

  public static GravitinoInfoProviderImpl getInstance() {
    return Holder.INSTANCE;
  }

  public static GravitinoInfoProviderImpl newInstanceForTest() {
    return new GravitinoInfoProviderImpl();
  }

  private GravitinoInfoProviderImpl() {}

  public boolean useGravitinoIdentifier() {
    try {
      boolean result = getGravitinoInfo().isUseGravitinoIdentifier();
      log.trace("useGravitinoIdentifier() = {}", result);
      return result;
    } catch (IllegalStateException e) {
      // Gravitino not available, default to false
      log.trace("Gravitino not available, useGravitinoIdentifier() = false");
      return false;
    }
  }

  public String getGravitinoCatalog(String originCatalogName) {
    try {
      String mappedName =
          getGravitinoInfo().getCatalogMapping().getOrDefault(originCatalogName, originCatalogName);
      if (!originCatalogName.equals(mappedName)) {
        log.trace("Catalog name mapped: {} -> {}", originCatalogName, mappedName);
      }
      return mappedName;
    } catch (IllegalStateException e) {
      // Gravitino not available, return original name
      log.trace("Gravitino not available, returning original catalog name: {}", originCatalogName);
      return originCatalogName;
    }
  }

  public String getMetalakeName() {
    Optional<String> metalake = getGravitinoInfo().getMetalake();
    if (!metalake.isPresent()) {
      throw new RuntimeException(
          "Gravitino metalake configuration not found. "
              + "Please set either 'spark.sql.gravitino.metalake' (for Gravitino connector) "
              + "or 'spark.hadoop.fs.gravitino.client.metalake' (for GVFS filesystem) "
              + "in your Spark configuration.");
    }
    return metalake.get();
  }

  /**
   * Gets Gravitino configuration info with caching. Configuration is loaded once and cached for the
   * lifetime of the provider instance. Uses double-checked locking for thread-safe lazy
   * initialization.
   *
   * @return cached GravitinoInfo instance
   */
  private GravitinoInfo getGravitinoInfo() {
    // First check without synchronization for performance
    if (gravitinoInfo.isPresent()) {
      log.trace("Returning cached Gravitino configuration");
      return gravitinoInfo.get();
    }

    // Synchronize only if not yet initialized
    synchronized (this) {
      // Double-check after acquiring lock
      if (gravitinoInfo.isPresent()) {
        log.trace("Returning cached Gravitino configuration (after lock)");
        return gravitinoInfo.get();
      }

      log.debug("Loading Gravitino configuration for the first time");
      long startTime = System.currentTimeMillis();
      GravitinoInfo info = doGetGravitinoInfo();
      gravitinoInfo = Optional.of(info);
      long duration = System.currentTimeMillis() - startTime;
      log.info("Gravitino configuration loaded and cached in {}ms", duration);
    }
    return gravitinoInfo.get();
  }

  private GravitinoInfo doGetGravitinoInfo() {
    log.debug("Attempting to load Gravitino configuration from {} providers", providers.size());

    for (GravitinoInfoProvider provider : providers) {
      log.debug("Checking provider: {}", provider.getClass().getSimpleName());
      try {
        if (provider.isAvailable()) {
          log.debug(
              "Provider {} is available, loading configuration",
              provider.getClass().getSimpleName());
          GravitinoInfo info = provider.getGravitinoInfo();
          log.info(
              "Loaded Gravitino configuration: metalake={}, useGravitinoIdentifier={}, catalogMappings={}",
              info.getMetalake().orElse("not set"),
              info.isUseGravitinoIdentifier(),
              info.getCatalogMapping());
          return info;
        } else {
          log.debug("Provider {} is not available", provider.getClass().getSimpleName());
        }
      } catch (Exception e) {
        log.debug(
            "Provider {} failed to load configuration: {}",
            provider.getClass().getSimpleName(),
            e.getMessage());
      }
    }

    log.error("No available Gravitino info provider found");
    throw new IllegalStateException(
        "Could not find Gravitino info provider. Ensure Spark is available in the classpath.");
  }

  /**
   * Clears the cached configuration. This method is primarily for testing purposes to allow
   * reloading configuration between tests.
   */
  public void clearCache() {
    synchronized (this) {
      log.debug("Clearing cached Gravitino configuration");
      gravitinoInfo = Optional.empty();
    }
  }

  /**
   * Checks if configuration is currently cached.
   *
   * @return true if configuration is cached, false otherwise
   */
  boolean isCached() {
    return gravitinoInfo.isPresent();
  }
}
