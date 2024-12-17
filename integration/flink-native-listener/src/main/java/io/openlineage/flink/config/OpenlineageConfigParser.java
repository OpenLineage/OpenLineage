/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.client.DefaultConfigPathProvider;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

/** Class responsible for parsing Flink configuration to extract Openlineage entries. */
@Slf4j
public class OpenlineageConfigParser {
  public static final String ARRAY_PREFIX_CHAR = "[";
  public static final String ARRAY_SUFFIX_CHAR = "]";
  public static final String ARRAY_ELEMENTS_SEPARATOR = ";";
  public static final Set<String> PROPERTIES_PREFIXES =
      new HashSet<>(
          Arrays.asList("transport.properties.", "transport.urlParams.", "transport.headers."));

  public static FlinkOpenLineageConfig parse(Configuration configuration) {
    // TRY READING CONFIG FROM FILE
    Optional<FlinkOpenLineageConfig> configFromFile = extractOpenLineageConfFromFile();

    FlinkOpenLineageConfig configFromFlinkConf = extractOpenLineageConfFromFlinkConf(configuration);
    FlinkOpenLineageConfig targetConfig;
    if (configFromFile.isPresent()) {
      targetConfig = configFromFile.get().mergeWith(configFromFlinkConf);
    } else {
      targetConfig = configFromFlinkConf;
    }

    return targetConfig;
  }

  private static Optional<FlinkOpenLineageConfig> extractOpenLineageConfFromFile() {
    Optional<FlinkOpenLineageConfig> configFromFile;
    try {
      configFromFile =
          Optional.of(
              OpenLineageClientUtils.loadOpenLineageConfigYaml(
                  new DefaultConfigPathProvider(), new TypeReference<FlinkOpenLineageConfig>() {}));
    } catch (OpenLineageClientException e) {
      log.info("Couldn't log config from file, will read it from FlinkConf");
      configFromFile = Optional.empty();
    }
    return configFromFile;
  }

  private static FlinkOpenLineageConfig extractOpenLineageConfFromFlinkConf(
      Configuration configuration) {
    // configuration
    List<String> configKeys =
        configuration.keySet().stream()
            .filter(key -> key.startsWith("openlineage."))
            .collect(Collectors.toList());

    ObjectMapper objectMapper = OpenLineageClientUtils.newObjectMapper();
    ObjectNode objectNode = objectMapper.createObjectNode();
    for (String configKey : configKeys) {
      ObjectNode nodePointer = objectNode;
      String value = configuration.get(ConfigOptions.key(configKey).stringType().noDefaultValue());

      if (StringUtils.isNotBlank(value)) {
        List<String> pathKeys = getJsonPath(configKey);
        List<String> nonLeafs = pathKeys.subList(0, pathKeys.size() - 1);
        String leaf = pathKeys.get(pathKeys.size() - 1);
        for (String node : nonLeafs) {
          if (nodePointer.get(node) == null) {
            nodePointer.putObject(node);
          }
          nodePointer = (ObjectNode) nodePointer.get(node);
        }
        if (isArrayType(value)) {
          ArrayNode arrayNode = nodePointer.putArray(leaf);
          String valueWithoutBrackets =
              isArrayType(value) ? value.substring(1, value.length() - 1) : value;
          Arrays.stream(valueWithoutBrackets.split(ARRAY_ELEMENTS_SEPARATOR))
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
          new TypeReference<FlinkOpenLineageConfig>() {});
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
            .orElseGet(() -> Arrays.asList(keyPath.replace("openlineage.", "").split("\\.")));
    return pathKeys;
  }

  private static boolean isArrayType(String value) {
    return value.startsWith(ARRAY_PREFIX_CHAR) && value.endsWith(ARRAY_SUFFIX_CHAR);
  }
}
