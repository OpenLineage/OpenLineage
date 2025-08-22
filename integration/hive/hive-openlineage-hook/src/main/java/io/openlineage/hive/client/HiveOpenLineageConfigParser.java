/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class HiveOpenLineageConfigParser {
  public static final String CONF_PREFIX = "hive.openlineage.";
  public static final Set<String> PROPERTIES_PREFIXES =
      new HashSet<>(
          Arrays.asList("transport.properties.", "transport.urlParams.", "transport.headers."));
  public static final String NAMESPACE_KEY = CONF_PREFIX + "namespace";
  public static final String JOB_NAME_KEY = CONF_PREFIX + "job.name";
  public static final String DATASET_LINEAGE_ENABLED_KEY = CONF_PREFIX + "datasetLineageEnabled";

  private static final ObjectMapper JSON = new ObjectMapper();

  public static String unescapeValue(String value) {
    if (!value.contains("&amp;") && !value.contains("&quot;")) {
      return value;
    }
    String unescapedValue = StringEscapeUtils.unescapeXml(value);
    return unescapeValue(unescapedValue);
  }

  public static HiveOpenLineageConfig extractFromHadoopConf(Configuration conf) {
    Set<String> configKeys =
        conf.getPropsWithPrefix(CONF_PREFIX).keySet().stream()
            .filter(
                key ->
                    key.startsWith("transport")
                        || key.startsWith("facets")
                        || key.startsWith("circuitBreaker"))
            .collect(Collectors.toSet());
    ObjectNode objectNode = JSON.createObjectNode();
    for (String key : configKeys) {
      ObjectNode nodePointer = objectNode;
      String possiblyEscapedValue = conf.get(CONF_PREFIX + key);
      String value = unescapeValue(possiblyEscapedValue);
      if (StringUtils.isNotBlank(value)) {
        List<String> pathKeys = getJsonPath(key);
        List<String> nonLeafs = pathKeys.subList(0, pathKeys.size() - 1);
        String leaf = pathKeys.get(pathKeys.size() - 1);
        for (String node : nonLeafs) {
          if (nodePointer.get(node) == null) {
            nodePointer.putObject(node);
          }
          nodePointer = (ObjectNode) nodePointer.get(node);
        }
        Object parsedValue = parseValue(value);
        if (parsedValue instanceof String) {
          nodePointer.put(leaf, (String) parsedValue);
        } else if (parsedValue instanceof JsonNode) {
          nodePointer.set(leaf, (JsonNode) parsedValue);
        } else {
          throw new IllegalArgumentException("Unexpected value: " + value);
        }
      }
    }
    try {
      return JSON.readValue(
          new ByteArrayInputStream(JSON.writeValueAsBytes(objectNode)),
          new TypeReference<HiveOpenLineageConfig>() {});
    } catch (IOException | IllegalArgumentException configException) {
      String errorMessage;
      try {
        errorMessage = "Failure to read the configuration: " + JSON.writeValueAsString(objectNode);
      } catch (JsonProcessingException jsonException) {
        // If we can't serialize the objectNode, fall back to a generic error message
        errorMessage =
            "Failure to read the configuration and unable to serialize the configuration object";
        // Combine both exceptions
        configException.addSuppressed(jsonException);
      }
      throw new IllegalArgumentException(errorMessage, configException);
    }
  }

  private static List<String> getJsonPath(String keyPath) {
    Optional<String> propertyPath =
        PROPERTIES_PREFIXES.stream().filter(keyPath::startsWith).findAny();
    return propertyPath
        .map(
            s -> {
              List<String> path = new ArrayList<>(Arrays.asList(s.split("\\.")));
              path.add(keyPath.replaceFirst(s, ""));
              return path;
            })
        .orElseGet(() -> Arrays.asList(keyPath.replace("openlineage.", "").split("\\.")));
  }

  private static Object parseValue(String value) {
    try {
      JsonNode jsonNode = JSON.readTree(value);
      if (jsonNode.isObject() || jsonNode.isArray()) {
        return jsonNode;
      } else {
        return jsonNode.asText();
      }
    } catch (Exception e) {
      return value;
    }
  }
}
