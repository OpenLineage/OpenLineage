/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.utils.gravitino;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Provider for Gravitino configuration from Spark session. Reads Gravitino-specific configuration
 * from Spark's runtime configuration using reflection to avoid direct Spark dependencies.
 */
@Slf4j
public class SparkGravitinoInfoProvider implements GravitinoInfoProvider {

  private static final String SPARK_SESSION_CLASS_NAME = "org.apache.spark.sql.SparkSession";
  private static final String SPARK_RUN_CONFIG_CLASS_NAME = "org.apache.spark.sql.RuntimeConfig";

  /** Configuration key for Gravitino metalake name used by GVFS filesystem */
  public static final String metalakeConfigKeyForFS = "spark.hadoop.fs.gravitino.client.metalake";

  /** Configuration key for Gravitino metalake name used by Gravitino Spark connector */
  public static final String metalakeConfigKeyForConnector = "spark.sql.gravitino.metalake";

  /**
   * Configuration key to control whether to use Gravitino identifier format. Default: true when not
   * set
   */
  public static final String useGravitinoConfigKey = "spark.sql.gravitino.useGravitinoIdentifier";

  /**
   * Configuration key for catalog name mappings. Format:
   * "spark_catalog1:gravitino_catalog1,spark_catalog2:gravitino_catalog2"
   */
  public static final String catalogMappingConfigKey = "spark.sql.gravitino.catalogMappings";

  private static final int CATALOG_MAPPING_PARTS = 2;

  @Override
  public boolean isAvailable() {
    try {
      SparkGravitinoInfoProvider.class.getClassLoader().loadClass(SPARK_SESSION_CLASS_NAME);
      log.debug("SparkSession class found, Gravitino info provider is available");
      return true;
    } catch (ClassNotFoundException e) {
      log.debug(
          "SparkSession class not found, Gravitino info provider is not available: {}",
          e.getMessage());
      return false;
    }
  }

  @Override
  public GravitinoInfo getGravitinoInfo() {
    return GravitinoInfo.builder()
        .useGravitinoIdentifier(getUseGravitinoIdentifier())
        .catalogMapping(getCatalogMapping())
        .metalake(getMetalake())
        .build();
  }

  /**
   * Determines whether to use Gravitino identifier format for lineage.
   *
   * @return true if Gravitino identifiers should be used (default), false otherwise
   */
  private boolean getUseGravitinoIdentifier() {
    String useGravitino = getSparkConfigValue(useGravitinoConfigKey);
    if (StringUtils.isBlank(useGravitino)) {
      log.debug("Configuration '{}' not set, defaulting to true", useGravitinoConfigKey);
      return true; // Default to true when not configured
    }
    boolean result = Boolean.valueOf(useGravitino);
    log.debug("Configuration '{}' = {}", useGravitinoConfigKey, result);
    return result;
  }

  /**
   * Parses catalog name mappings from Spark configuration. Mappings allow translating Spark catalog
   * names to Gravitino catalog names.
   *
   * @return Map of Spark catalog name to Gravitino catalog name, empty if not configured
   * @throws IllegalArgumentException if mapping format is invalid
   */
  private Map<String, String> getCatalogMapping() {
    String catalogMapping = getSparkConfigValue(catalogMappingConfigKey);
    if (StringUtils.isBlank(catalogMapping)) {
      log.debug("No catalog mappings configured");
      return new HashMap<>();
    }

    log.debug("Parsing catalog mappings from configuration: {}", catalogMapping);
    Map<String, String> catalogMaps = new HashMap<>();

    Arrays.stream(catalogMapping.split(","))
        .forEach(
            item -> {
              String[] kv = item.split(":");
              if (kv.length == CATALOG_MAPPING_PARTS) {
                String key = kv[0].trim();
                String value = kv[1].trim();
                if (!key.isEmpty() && !value.isEmpty()) {
                  catalogMaps.put(key, value);
                  log.debug("Added catalog mapping: {} -> {}", key, value);
                } else {
                  log.warn("Skipping catalog mapping with empty key or value: '{}'", item.trim());
                }
              } else if (!item.trim().isEmpty()) {
                log.error("Invalid catalog mapping format: '{}'", item.trim());
                throw new IllegalArgumentException(
                    String.format(
                        "Invalid catalog mapping format: '%s'. "
                            + "Expected format: 'catalog1:gravitino1,catalog2:gravitino2'. "
                            + "Each mapping must be in 'key:value' format.",
                        item.trim()));
              }
            });

    log.info("Loaded {} catalog mappings: {}", catalogMaps.size(), catalogMaps);
    return catalogMaps;
  }

  /**
   * Retrieves the Gravitino metalake name from Spark configuration. Checks connector configuration
   * first, then falls back to filesystem configuration.
   *
   * @return Optional containing metalake name if found, empty otherwise
   */
  private Optional<String> getMetalake() {
    try {
      return Optional.ofNullable(tryGetMetalake());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  /**
   * Attempts to get metalake name, checking connector config first, then filesystem config.
   *
   * @return metalake name or null if not found
   */
  private String tryGetMetalake() {
    // Connector config takes precedence
    String metalake = getSparkConfigValue(metalakeConfigKeyForConnector);
    if (metalake != null) {
      log.debug(
          "Found metalake from connector configuration '{}': {}",
          metalakeConfigKeyForConnector,
          metalake);
      return metalake;
    }

    // Fall back to filesystem config
    metalake = getSparkConfigValue(metalakeConfigKeyForFS);
    if (metalake != null) {
      log.debug(
          "Found metalake from filesystem configuration '{}': {}",
          metalakeConfigKeyForFS,
          metalake);
    } else {
      log.warn(
          "Metalake not found in either '{}' or '{}'",
          metalakeConfigKeyForConnector,
          metalakeConfigKeyForFS);
    }
    return metalake;
  }

  /**
   * Retrieves a configuration value from the active Spark session using reflection. This approach
   * avoids direct Spark dependencies in the client module.
   *
   * @param configKey the Spark configuration key to retrieve
   * @return the configuration value, or null if not set
   */
  @SneakyThrows
  private String getSparkConfigValue(String configKey) {
    log.trace("Reading Spark configuration key: {}", configKey);

    Class<?> sparkSessionClass = Class.forName(SPARK_SESSION_CLASS_NAME);

    // SparkSession s = SparkSession.active()
    Method activeMethod = sparkSessionClass.getMethod("active");
    Object sparkSessionInstance = activeMethod.invoke(null);

    // RuntimeConfig config = s.conf()
    Method confMethod = sparkSessionClass.getMethod("conf");
    Object sparkConfInstance = confMethod.invoke(sparkSessionInstance);

    // config.get(configKey, null)
    Class<?> runConfigClass = Class.forName(SPARK_RUN_CONFIG_CLASS_NAME);
    Method getMethod = runConfigClass.getMethod("get", String.class, String.class);
    String value = (String) getMethod.invoke(sparkConfInstance, configKey, null);

    log.trace("Configuration '{}' = {}", configKey, value != null ? value : "<not set>");
    return value;
  }
}
