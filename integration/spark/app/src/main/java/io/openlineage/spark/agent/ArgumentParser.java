/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.util.SparkConfUtils.findSparkConfigKey;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.OpenLineageYaml;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import scala.Tuple2;

@AllArgsConstructor
@Slf4j
@Getter
@ToString
@Builder
public class ArgumentParser {

  public static final String SPARK_CONF_NAMESPACE = "spark.openlineage.namespace";
  public static final String SPARK_CONF_JOB_NAME = "spark.openlineage.parentJobName";
  public static final String SPARK_CONF_PARENT_RUN_ID = "spark.openlineage.parentRunId";
  public static final String SPARK_CONF_APP_NAME = "spark.openlineage.appName";
  public static final String SPARK_CONF_DISABLED_FACETS = "spark.openlineage.facets.disabled";
  public static final String DEFAULT_DISABLED_FACETS = "[spark_unknown;]";
  public static final String ARRAY_PREFIX_CHAR = "[";
  public static final String ARRAY_SUFFIX_CHAR = "]";
  public static final String DISABLED_FACETS_SEPARATOR = ";";
  public static final String SPARK_CONF_TRANSPORT_TYPE = "spark.openlineage.transport.type";
  public static final String SPARK_CONF_HTTP_URL = "spark.openlineage.transport.url";
  public static final Set<String> PROPERTIES_PREFIXES =
      new HashSet<>(Arrays.asList("transport.properties.", "transport.urlParams."));
  public static final String SPARK_CONF_CUSTOM_ENVIRONMENT_VARIABLES =
      "spark.openlineage.facets.custom_environment_variables";

  @Builder.Default private String namespace = "default";
  @Builder.Default private String jobName = "default";
  @Builder.Default private String parentRunId = null;
  @Builder.Default private String appName = null;
  @Builder.Default private OpenLineageYaml openLineageYaml = new OpenLineageYaml();

  public static ArgumentParser parse(SparkConf conf) {
    ArgumentParserBuilder builder = ArgumentParser.builder();
    conf.setIfMissing(SPARK_CONF_DISABLED_FACETS, DEFAULT_DISABLED_FACETS);
    conf.setIfMissing(SPARK_CONF_TRANSPORT_TYPE, "console");

    if (conf.get(SPARK_CONF_TRANSPORT_TYPE).equals("http")) {
      findSparkConfigKey(conf, SPARK_CONF_HTTP_URL)
          .ifPresent(url -> UrlParser.parseUrl(url).forEach(conf::set));
    }
    findSparkConfigKey(conf, SPARK_CONF_APP_NAME)
        .filter(str -> !str.isEmpty())
        .ifPresent(builder::appName);
    findSparkConfigKey(conf, SPARK_CONF_NAMESPACE).ifPresent(builder::namespace);
    findSparkConfigKey(conf, SPARK_CONF_JOB_NAME).ifPresent(builder::jobName);
    findSparkConfigKey(conf, SPARK_CONF_PARENT_RUN_ID).ifPresent(builder::parentRunId);
    builder.openLineageYaml(extractOpenlineageConfFromSparkConf(conf));
    return builder.build();
  }

  public static OpenLineageYaml extractOpenlineageConfFromSparkConf(SparkConf conf) {
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
            || SPARK_CONF_DISABLED_FACETS.equals("spark.openlineage." + keyPath)) {
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
      return OpenLineageClientUtils.loadOpenLineageYaml(
          new ByteArrayInputStream(objectMapper.writeValueAsBytes(objectNode)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<Tuple2<String, String>> filterProperties(SparkConf conf) {
    return Arrays.stream(conf.getAllWithPrefix("spark.openlineage."))
        .filter(e -> e._1.startsWith("transport") || e._1.startsWith("facets"))
        .collect(Collectors.toList());
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
    return value.startsWith(ARRAY_PREFIX_CHAR)
        && value.endsWith(ARRAY_SUFFIX_CHAR)
        && value.contains(DISABLED_FACETS_SEPARATOR);
  }
}
