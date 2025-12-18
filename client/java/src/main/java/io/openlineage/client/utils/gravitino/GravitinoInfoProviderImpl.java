/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.utils.gravitino;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GravitinoInfoProviderImpl implements GravitinoInfoProvider {
  private volatile Optional<GravitinoInfo> gravitinoInfo = Optional.empty();
  private final List<GravitinoInfoProvider> providers = loadProviders();

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

  /**
   * Loads all available GravitinoInfoProvider implementations using ServiceLoader.
   * This allows different modules (like Spark integration) to provide their own implementations
   * without creating direct dependencies.
   *
   * @return List of available providers
   */
  private static List<GravitinoInfoProvider> loadProviders() {
    ServiceLoader<GravitinoInfoProvider> loader = ServiceLoader.load(GravitinoInfoProvider.class);
    List<GravitinoInfoProvider> providers = new ArrayList<>();
    
    StreamSupport.stream(loader.spliterator(), false)
        .forEach(provider -> {
          log.debug("Discovered GravitinoInfoProvider: {}", provider.getClass().getName());
          providers.add(provider);
        });
    
    if (providers.isEmpty()) {
      log.warn("No GravitinoInfoProvider implementations found via ServiceLoader");
    } else {
      log.debug("Loaded {} GravitinoInfoProvider implementations", providers.size());
    }
    
    return providers;
  }





  public String getMetalakeName() {
    Optional<String> metalake = getGravitinoInfoInternal().getMetalake();
    if (!metalake.isPresent()) {
      throw new RuntimeException(
          "Gravitino metalake configuration not found. "
              + "Please set either 'spark.sql.gravitino.metalake' (for Gravitino connector) "
              + "or 'spark.hadoop.fs.gravitino.client.metalake' (for GVFS filesystem) "
              + "in your Spark configuration.");
    }
    return metalake.get();
  }

  @Override
  public boolean isAvailable() {
    try {
      for (GravitinoInfoProvider provider : providers) {
        if (provider.isAvailable()) {
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      log.debug("Error checking Gravitino availability: {}", e.getMessage());
      return false;
    }
  }

  @Override
  public GravitinoInfo getGravitinoInfo() {
    return getGravitinoInfoInternal();
  }

  /**
   * Gets Gravitino configuration info with caching. Configuration is loaded once and cached for the
   * lifetime of the provider instance. Uses double-checked locking for thread-safe lazy
   * initialization.
   *
   * @return cached GravitinoInfo instance
   */
  private GravitinoInfo getGravitinoInfoInternal() {
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
              "Loaded Gravitino configuration: metalake={}",
              info.getMetalake().orElse("not set"));
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
