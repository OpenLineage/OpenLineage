/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.gravitino.GravitinoInfo;
import io.openlineage.client.utils.gravitino.GravitinoInfoProvider;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider for Gravitino configuration from Spark session. Reads Gravitino-specific configuration
 * from Spark's runtime configuration.
 */
public class SparkGravitinoInfoProvider implements GravitinoInfoProvider {
  private static final Logger log = LoggerFactory.getLogger(SparkGravitinoInfoProvider.class);

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
      // Check if there's an active Spark session and if any Gravitino config is present
      return hasActiveSparkSessionWithGravitinoConfig();
    } catch (Exception e) {
      log.debug("Error checking Gravitino availability: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Checks if there's an active Spark session with at least one Gravitino configuration key set.
   * 
   * @return true if Spark session is active and has Gravitino config, false otherwise
   */
  private boolean hasActiveSparkSessionWithGravitinoConfig() {
    Optional<SparkSession> sessionOpt = SparkSessionUtils.activeSession();
    
    if (!sessionOpt.isPresent()) {
      log.debug("No active Spark session found");
      return false;
    }
    
    SparkSession session = sessionOpt.get();
    
    // Check if any Gravitino configuration is present
    boolean hasMetalakeConfig = getSparkConfigValue(session, metalakeConfigKeyForConnector) != null 
        || getSparkConfigValue(session, metalakeConfigKeyForFS) != null;
    boolean hasGravitinoConfig = getSparkConfigValue(session, useGravitinoConfigKey) != null;
    boolean hasCatalogMapping = getSparkConfigValue(session, catalogMappingConfigKey) != null;
    
    boolean available = hasMetalakeConfig || hasGravitinoConfig || hasCatalogMapping;
    
    if (available) {
      log.debug("Active Spark session found with Gravitino configuration");
    } else {
      log.debug("Active Spark session found but no Gravitino configuration detected");
    }
    
    return available;
  }

  @Override
  public GravitinoInfo getGravitinoInfo() {
    Optional<SparkSession> sessionOpt = SparkSessionUtils.activeSession();
    if (!sessionOpt.isPresent()) {
      throw new IllegalStateException("No active Spark session found");
    }
    
    SparkSession session = sessionOpt.get();
    return GravitinoInfo.builder()
        .useGravitinoIdentifier(getUseGravitinoIdentifier(session))
        .catalogMapping(getCatalogMapping(session))
        .metalake(getMetalake(session))
        .build();
  }

  /**
   * Determines whether to use Gravitino identifier format for lineage.
   *
   * @param session the active Spark session
   * @return true if Gravitino identifiers should be used, false otherwise (default)
   */
  private boolean getUseGravitinoIdentifier(SparkSession session) {
    String useGravitino = getSparkConfigValue(session, useGravitinoConfigKey);
    if (StringUtils.isBlank(useGravitino)) {
      log.debug("Configuration '{}' not set, defaulting to false", useGravitinoConfigKey);
      return false; // Default to false when not configured
    }
    boolean result = Boolean.valueOf(useGravitino);
    log.debug("Configuration '{}' = {}", useGravitinoConfigKey, result);
    return result;
  }

  /**
   * Parses catalog name mappings from Spark configuration. Mappings allow translating Spark catalog
   * names to Gravitino catalog names.
   *
   * @param session the active Spark session
   * @return Map of Spark catalog name to Gravitino catalog name, empty if not configured
   * @throws IllegalArgumentException if mapping format is invalid
   */
  private Map<String, String> getCatalogMapping(SparkSession session) {
    String catalogMapping = getSparkConfigValue(session, catalogMappingConfigKey);
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
   * @param session the active Spark session
   * @return Optional containing metalake name if found, empty otherwise
   */
  private Optional<String> getMetalake(SparkSession session) {
    // Connector config takes precedence
    String metalake = getSparkConfigValue(session, metalakeConfigKeyForConnector);
    if (metalake != null) {
      log.debug(
          "Found metalake from connector configuration '{}': {}",
          metalakeConfigKeyForConnector,
          metalake);
      return Optional.of(metalake);
    }

    // Fall back to filesystem config
    metalake = getSparkConfigValue(session, metalakeConfigKeyForFS);
    if (metalake != null) {
      log.debug(
          "Found metalake from filesystem configuration '{}': {}",
          metalakeConfigKeyForFS,
          metalake);
      return Optional.of(metalake);
    } else {
      log.warn(
          "Metalake not found in either '{}' or '{}'",
          metalakeConfigKeyForConnector,
          metalakeConfigKeyForFS);
      return Optional.empty();
    }
  }

  /**
   * Retrieves a configuration value from the Spark session.
   *
   * @param session the Spark session
   * @param configKey the Spark configuration key to retrieve
   * @return the configuration value, or null if not set
   */
  private String getSparkConfigValue(SparkSession session, String configKey) {
    try {
      log.trace("Reading Spark configuration key: {}", configKey);
      String value = session.conf().get(configKey, null);
      log.trace("Configuration '{}' = {}", configKey, value != null ? value : "<not set>");
      return value;
    } catch (Exception e) {
      log.debug("Failed to read Spark configuration '{}': {}", configKey, e.getMessage());
      return null;
    }
  }
}