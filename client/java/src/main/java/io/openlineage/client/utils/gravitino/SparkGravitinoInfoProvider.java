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
import org.apache.commons.lang3.StringUtils;

public class SparkGravitinoInfoProvider implements GravitinoInfoProvider {

  private static final String SPARK_SESSION_CLASS_NAME = "org.apache.spark.sql.SparkSession";
  private static final String SPARK_RUN_CONFIG_CLASS_NAME = "org.apache.spark.sql.RuntimeConfig";
  public static final String metalakeConfigKeyForFS = "spark.hadoop.fs.gravitino.client.metalake";
  public static final String metalakeConfigKeyForConnector = "spark.sql.gravitino.metalake";

  public static final String useGravitinoConfigKey = "spark.sql.gravitino.useGravitinoIdentifier";
  public static final String catalogMappingConfigKey = "spark.sql.gravitino.catalogMappings";

  @Override
  public boolean isAvailable() {
    try {
      SparkGravitinoInfoProvider.class.getClassLoader().loadClass(SPARK_SESSION_CLASS_NAME);
      return true;
    } catch (ClassNotFoundException e) {
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

  private boolean getUseGravitinoIdentifier() {
    String useGravitino = getSparkConfigValue(useGravitinoConfigKey);
    if (StringUtils.isBlank(useGravitino)) {
      return true;
    }
    return Boolean.valueOf(useGravitino);
  }

  private Map<String, String> getCatalogMapping() {
    String catalogMapping = getSparkConfigValue(catalogMappingConfigKey);
    if (StringUtils.isBlank(catalogMapping)) {
      return new HashMap<>();
    }

    Map<String, String> catalogMaps = new HashMap<>();
    Arrays.stream(catalogMapping.split(","))
        .forEach(
            item -> {
              String[] kv = item.split(":");
              if (kv.length == 2) {
                catalogMaps.put(kv[0].trim(), kv[1].trim());
              }
            });
    return catalogMaps;
  }

  private Optional<String> getMetalake() {
    try {
      return Optional.ofNullable(tryGetMetalake());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private String tryGetMetalake() {
    String metalake = getSparkConfigValue(metalakeConfigKeyForConnector);
    if (metalake != null) {
      return metalake;
    }
    return getSparkConfigValue(metalakeConfigKeyForFS);
  }

  @SneakyThrows
  private String getSparkConfigValue(String configKey) {
    Class<?> sparkSessionClass = Class.forName(SPARK_SESSION_CLASS_NAME);

    // SparkSession s = SparkSession.active()
    Method activeMethod = sparkSessionClass.getMethod("active");
    Object sparkSessionInstance = activeMethod.invoke(null);

    // RuntimeConfig config = s.conf()
    Method confMethod = sparkSessionClass.getMethod("conf");
    Object sparkConfInstance = confMethod.invoke(sparkSessionInstance);

    // config.get(metalakeConfigKey, null)
    Class<?> runConfigClass = Class.forName(SPARK_RUN_CONFIG_CLASS_NAME);
    Method getMethod = runConfigClass.getMethod("get", String.class, String.class);
    return (String) getMethod.invoke(sparkConfInstance, configKey, null);
  }
}
