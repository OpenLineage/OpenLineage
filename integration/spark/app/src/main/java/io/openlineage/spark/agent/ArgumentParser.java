/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.util.SparkConfUtils.findSparkConfigKey;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.client.DefaultConfigPathProvider;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.ConsoleConfig;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Map;
import java.util.HashMap; 
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import scala.Tuple2;

@Slf4j
@ToString
public class ArgumentParser {

  public static final String SPARK_CONF_NAMESPACE = "spark.openlineage.namespace";

  public static final String SPARK_CONF_PARENT_JOB_NAMESPACE =
      "spark.openlineage.parentJobNamespace";
  
  public static final String SPARK_CONF_PARENT_JOB_NAME = "spark.openlineage.parentJobName";

  public static final String SPARK_CONF_PARENT_RUN_ID = "spark.openlineage.parentRunId";

  public static final String SPARK_CONF_APP_NAME = "spark.openlineage.appName";

  public static final String ARRAY_PREFIX_CHAR = "[";
  public static final String ARRAY_SUFFIX_CHAR = "]";
  public static final String DISABLED_FACETS_SEPARATOR = ";";

  public static final String SPARK_CONF_TRANSPORT_TYPE = "spark.openlineage.transport.type";

  public static final String SPARK_CONF_HTTP_URL = "spark.openlineage.transport.url";

  public static final String SPARK_CONF_JOB_NAME_APPEND_DATASET_NAME =
      "spark.openlineage.jobName.appendDatasetName";

  public static final String SPARK_CONF_JOB_NAME_REPLACE_DOT_WITH_UNDERSCORE =
      "spark.openlineage.jobName.replaceDotWithUnderscore";
  
  private static final String SPARK_CONF_FACETS_DISABLED = "spark.openlineage.facets.disabled";

  private static final String SPARK_CONF_DEBUG_FACET = "spark.openlineage.debugFacet";

  private static final String SPARK_TEST_EXTENSION_PROVIDER =
      "spark.openlineage.testExtensionProvider";
  
  private static final String VAR_ENV_OPENLINEAGE_APP_NAME = "OPENLINEAGE_APP_NAME";

  private static final String VAR_ENV_OPENLINEAGE_FACETS_DISABLED = "OPENLINEAGE_FACETS__DISABLED";

  public static final Set<String> PROPERTIES_PREFIXES =
      new HashSet<>(
          Arrays.asList("transport.properties.", "transport.urlParams.", "transport.headers."));

  private static final String disabledFacetsSeparator = ";";

  public static SparkOpenLineageConfig parse(SparkConf conf) {
    // TRY READING CONFIG FROM FILE
    Optional<SparkOpenLineageConfig> configFromFile = extractOpenLineageConfFromFile();

    if ("http".equals(conf.get(SPARK_CONF_TRANSPORT_TYPE, ""))) {
      findSparkConfigKey(conf, SPARK_CONF_HTTP_URL)
          .ifPresent(url -> UrlParser.parseUrl(url).forEach(conf::set));
    }

    // TRY READING CONFIG FROM ENV VAR
    Optional <SparkOpenLineageConfig> configFromEnvVar = extractOpenLineageConfFromEnvVar();

    SparkOpenLineageConfig configFromSparkConf = extractOpenLineageConfFromSparkConf(conf);

    SparkOpenLineageConfig targetConfig;
    if (configFromFile.isPresent() && configFromEnvVar.isPresent()) {
      targetConfig = configFromFile.get().mergeWith(configFromEnvVar.get()).mergeWith(configFromSparkConf);
    }
    else if (configFromFile.isPresent()) {
      targetConfig = configFromFile.get().mergeWith(configFromSparkConf);
    }
    else if (configFromEnvVar.isPresent()) {
      targetConfig = configFromEnvVar.get().mergeWith(configFromSparkConf);
    } else {
      targetConfig = configFromSparkConf;
    }

    // SET DEFAULTS
    if (targetConfig.getTransportConfig() == null) {
      targetConfig.setTransportConfig(new ConsoleConfig());
    }
    
    extractSparkSpecificConfigEntriesFromSparkConf(conf, targetConfig);
    return targetConfig;
  }

  private static Optional<SparkOpenLineageConfig> extractOpenLineageConfFromFile() {
    Optional<SparkOpenLineageConfig> configFromFile;
    try {
      configFromFile =
          Optional.of(
              OpenLineageClientUtils.loadOpenLineageConfigYaml(
                  new DefaultConfigPathProvider(), new TypeReference<SparkOpenLineageConfig>() {}));
    } catch (OpenLineageClientException e) {
      log.info("Couldn't log config from file, will read it from SparkConf");
      configFromFile = Optional.empty();
    }
    return configFromFile;
  }

  private static void extractSparkSpecificConfigEntriesFromSparkConf(
      SparkConf conf, SparkOpenLineageConfig config) {
    findSparkConfigKey(conf, SPARK_CONF_APP_NAME)
        .filter(str -> !str.isEmpty())
        .ifPresent(config::setOverriddenAppName);

    findSparkConfigKey(conf, SPARK_CONF_NAMESPACE).ifPresent(config::setNamespace);
    findSparkConfigKey(conf, SPARK_CONF_PARENT_JOB_NAME).ifPresent(config::setParentJobName);
    findSparkConfigKey(conf, SPARK_CONF_PARENT_JOB_NAMESPACE)
        .ifPresent(config::setParentJobNamespace);
    findSparkConfigKey(conf, SPARK_CONF_PARENT_RUN_ID).ifPresent(config::setParentRunId);
    findSparkConfigKey(conf, SPARK_CONF_DEBUG_FACET).ifPresent(config::setDebugFacet);
    findSparkConfigKey(conf, SPARK_TEST_EXTENSION_PROVIDER)
        .ifPresent(config::setTestExtensionProvider);
    findSparkConfigKey(conf, SPARK_CONF_JOB_NAME_APPEND_DATASET_NAME)
        .map(Boolean::valueOf)
        .ifPresent(v -> config.getJobName().setAppendDatasetName(v));
    findSparkConfigKey(conf, SPARK_CONF_JOB_NAME_REPLACE_DOT_WITH_UNDERSCORE)
        .map(Boolean::valueOf)
        .ifPresent(v -> config.getJobName().setReplaceDotWithUnderscore(v));
    findSparkConfigKey(conf, SPARK_CONF_FACETS_DISABLED)
        .map(s -> s.replace("[", "").replace("]", ""))
        .map(
            s ->
                Stream.of(s.split(disabledFacetsSeparator))
                    .filter(StringUtils::isNotBlank)
                    .toArray(String[]::new))
        .ifPresent(a -> config.getFacetsConfig().setDisabledFacets(a));
  }

  private static Optional<SparkOpenLineageConfig> extractOpenLineageConfFromEnvVar() {
      Map<String, String> envVars = System.getenv();
      ObjectMapper objectMapper = new ObjectMapper();
      ObjectNode objectNode = objectMapper.createObjectNode();

      for (Map.Entry<String, String> entry : envVars.entrySet()) {
          String envKey = entry.getKey();
          String value = entry.getValue();

          if (envKey.startsWith("OPENLINEAGE_")) {
              if (VAR_ENV_OPENLINEAGE_APP_NAME.equals(envKey)) {
                  objectNode.put("overriddenAppName", value);
              } else {
                  String keyPath = convertEnvKeyToConfKey(envKey);
                  List<String> pathKeys = getJsonPath(keyPath);
                  ObjectNode nodePointer = objectNode;

                  for (int i = 0; i < pathKeys.size() - 1; i++) {
                      String node = pathKeys.get(i);
                      if (nodePointer.get(node) == null) {
                          nodePointer.putObject(node);
                      }
                      nodePointer = (ObjectNode) nodePointer.get(node);
                  }

                  String leaf = pathKeys.get(pathKeys.size() - 1);
                  if (isArrayType(value) || "OPENLINEAGE_FACETS__DISABLED".equals(envKey)) {
                      ArrayNode arrayNode = nodePointer.putArray(leaf);
                      String valueWithoutBrackets = isArrayType(value) ? value.substring(1, value.length() - 1) : value;
                      Arrays.stream(valueWithoutBrackets.split(DISABLED_FACETS_SEPARATOR))
                          .filter(StringUtils::isNotBlank)
                          .forEach(arrayNode::add);
                  } else {
                      nodePointer.put(leaf, value);
                  }
              }
          }
      }

      try {
          return Optional.of(OpenLineageClientUtils.loadOpenLineageConfigYaml(
              new ByteArrayInputStream(objectMapper.writeValueAsBytes(objectNode)),
              new TypeReference<SparkOpenLineageConfig>() {}));
      } catch (IOException e) {
          log.warn("Failed to parse OpenLineage configuration from environment variables", e);
          return Optional.empty();
      }
  }

  private static String convertEnvKeyToConfKey(String envKey) {
      // Remove "OPENLINEAGE_" prefix
      String withoutPrefix = envKey.substring("OPENLINEAGE_".length());
      
      // Split by double underscores
      String[] mainParts = withoutPrefix.split("__");
      
      StringBuilder result = new StringBuilder();
      
      for (int i = 0; i < mainParts.length; i++) {
          if (i > 0) {
              result.append(".");
          }
          
          // Process each main part
          String[] subParts = mainParts[i].split("_");
          for (int j = 0; j < subParts.length; j++) {
              String part = subParts[j].toLowerCase();
              if (j > 0) {
                  // CamelCase for parts split by single underscore
                  part = Character.toUpperCase(part.charAt(0)) + part.substring(1);
              }
              result.append(part);
          }
      }
      return result.toString();
  }

  /**
   * Method iterates through SparkConf entries prefixed with `spark.openlineage` and rearranges them
   * into Jackson ObjectNode object, which is then loaded within {@link OpenLineageClientUtils} to
   * {@link SparkOpenLineageConfig} instance.
   *
   * @param conf
   * @return
   */
  private static SparkOpenLineageConfig extractOpenLineageConfFromSparkConf(SparkConf conf) {
    List<Tuple2<String, String>> properties = filterProperties(conf);
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode objectNode = objectMapper.createObjectNode();
    for (Tuple2<String, String> c : properties) {
      ObjectNode nodePointer = objectNode;
      String keyPath = c._1;
      String value = c._2;
      if (StringUtils.isNotBlank(value)) {
        List<String> pathKeys = getJsonPath(keyPath);
        List<String> nonLeafs = pathKeys.subList(0, pathKeys.size() - 1);
        String leaf = pathKeys.get(pathKeys.size() - 1);
        for (String node : nonLeafs) {
          if (nodePointer.get(node) == null) {
            nodePointer.putObject(node);
          }
          nodePointer = (ObjectNode) nodePointer.get(node);
        }
        if (isArrayType(value)
            || SPARK_CONF_FACETS_DISABLED.equals("spark.openlineage." + keyPath)) {
          ArrayNode arrayNode = nodePointer.putArray(leaf);
          String valueWithoutBrackets =
              isArrayType(value) ? value.substring(1, value.length() - 1) : value;
          Arrays.stream(valueWithoutBrackets.split(DISABLED_FACETS_SEPARATOR))
              .filter(StringUtils::isNotBlank)
              .forEach(arrayNode::add);
        } else {
          nodePointer.put(leaf, value);
        }
      }
    }

    try {
      return OpenLineageClientUtils.loadOpenLineageConfigYaml(
          new ByteArrayInputStream(objectMapper.writeValueAsBytes(objectNode)),
          new TypeReference<SparkOpenLineageConfig>() {});
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<Tuple2<String, String>> filterProperties(SparkConf conf) {
    return Arrays.stream(conf.getAllWithPrefix("spark.openlineage.")).collect(Collectors.toList());
  }

  private static List<String> getJsonPath(String keyPath) {
    Optional<String> propertyPath =
        PROPERTIES_PREFIXES.stream().filter(keyPath::startsWith).findAny();
    List<String> pathKeys =
        propertyPath
            .map(
                s -> {
                  List<String> path = new ArrayList<>(Arrays.asList(s.split("\\.")));
                  path.add(keyPath.replaceFirst(s, ""));
                  return path;
                })
            .orElseGet(() -> Arrays.asList(keyPath.split("\\.")));
    return pathKeys;
  }

  private static boolean isArrayType(String value) {
    return value.startsWith(ARRAY_PREFIX_CHAR) && value.endsWith(ARRAY_SUFFIX_CHAR);
  }
}
