/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openlineage.hive.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.OpenLineageYaml;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class HiveOpenLineageConfigParser {
  public static final String CONF_PREFIX = "hive.openlineage.";
  public static final String ARRAY_PREFIX_CHAR = "[";
  public static final String ARRAY_SUFFIX_CHAR = "]";
  public static final String ARRAY_ELEMENTS_SEPARATOR = ";";
  public static final Set<String> PROPERTIES_PREFIXES =
      new HashSet<>(
          Arrays.asList("transport.properties.", "transport.urlParams.", "transport.headers."));
  public static final String NAMESPACE = CONF_PREFIX + "namespace";

  public static OpenLineageYaml extractOpenLineageYaml(Configuration conf) {
    Set<String> configKeys =
        conf.getPropsWithPrefix(CONF_PREFIX).keySet().stream()
            .filter(
                key ->
                    key.startsWith("transport")
                        || key.startsWith("facets")
                        || key.startsWith("circuitBreaker"))
            .collect(Collectors.toSet());
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode objectNode = objectMapper.createObjectNode();
    for (String key : configKeys) {
      ObjectNode nodePointer = objectNode;
      String value = conf.get(CONF_PREFIX + key);
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
      return OpenLineageClientUtils.loadOpenLineageYaml(
          new ByteArrayInputStream(objectMapper.writeValueAsBytes(objectNode)));
    } catch (IOException e) {
      throw new RuntimeException(e);
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

  private static boolean isArrayType(String value) {
    return value.startsWith(ARRAY_PREFIX_CHAR)
        && value.endsWith(ARRAY_SUFFIX_CHAR)
        && value.contains(ARRAY_ELEMENTS_SEPARATOR);
  }
}
