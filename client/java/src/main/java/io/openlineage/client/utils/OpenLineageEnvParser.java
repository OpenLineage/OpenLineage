/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.client.Environment;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OpenLineageEnvParser {

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String PREFIX = "OPENLINEAGE__";

  /**
   * Parses all environment variables starting with OPENLINEAGE__ into a single nested JSON
   * structure.
   *
   * @return A single nested JSON structure with all matching environment variables.
   * @throws JsonProcessingException
   */
  public static String parseAllOpenLineageEnvVars() throws JsonProcessingException {
    ObjectNode root = objectMapper.createObjectNode();

    // Filter all environment variables starting with 'OPENLINEAGE__'
    Map<String, String> env = Environment.getAllEnvironmentVariables();
    env.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(PREFIX))
        .sorted(Map.Entry.comparingByKey(Comparator.reverseOrder()))
        .map(
            entry -> {
              String envKey = entry.getKey();
              String envValue = entry.getValue();
              try {
                // Parse the individual environment variable into a nested structure
                return parseEnvToNestedJson(envKey, envValue);
              } catch (Exception e) {
                log.warn("Failed to parse environment variable: {}", envKey);
                return null;
              }
            })
        .filter(Objects::nonNull) // Filter out any failed parsing attempts
        .forEach(parsedJson -> merge(root, parsedJson));
    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
  }

  /**
   * Parses a single environment variable key and value into a nested JSON structure.
   *
   * @param envKey The environment variable key.
   * @param envValue The environment variable value.
   * @return A nested JSON structure based on the key.
   */
  private static ObjectNode parseEnvToNestedJson(String envKey, String envValue) throws Exception {
    // Remove the 'OPENLINEAGE__' prefix
    String strippedKey = envKey.substring(PREFIX.length());

    // Split the remaining key on '__' into a List<String>
    List<String> keyParts =
        Stream.of(strippedKey.split("__"))
            .map(String::toLowerCase) // Lowercase the keys if needed
            .map(OpenLineageEnvParser::convertSnakeCaseToCamelCase)
            .collect(Collectors.toList());

    // Create the final value node. If it's valid JSON, parse it; otherwise, treat
    // it as a string.
    JsonNode valueNode;
    try {
      valueNode = objectMapper.readTree(envValue);
    } catch (JsonProcessingException e) {
      // If it's not valid JSON, treat it as a plain string
      valueNode = objectMapper.convertValue(envValue, JsonNode.class);
    }

    // Create nested structure based on keyParts
    ObjectNode root = objectMapper.createObjectNode();
    ObjectNode currentNode = root;

    // Create nested objects for each key part except the last
    for (int i = 0; i < keyParts.size() - 1; i++) {
      ObjectNode nextNode = objectMapper.createObjectNode();
      currentNode.set(keyParts.get(i), nextNode); // Use List's get() method
      currentNode = nextNode;
    }

    // Set the final value at the last part of the key
    currentNode.set(keyParts.get(keyParts.size() - 1), valueNode);

    return root;
  }

  /**
   * Converts a string from snake_case to camelCase.
   *
   * @param value The input string in snake_case.
   * @return The input string converted to camelCase.
   */
  private static String convertSnakeCaseToCamelCase(String value) {
    if (value.indexOf('_') == -1) {
      return value;
    }

    String initialPart = value.substring(0, value.indexOf('_'));

    String secondPart =
        Arrays.stream(value.substring(value.indexOf('_') + 1).split("_"))
            .map(s -> Character.toUpperCase(s.charAt(0)) + s.substring(1))
            .collect(Collectors.joining());

    return initialPart + secondPart;
  }

  /**
   * Merges two ObjectNodes into one.
   *
   * @param mainNode The main ObjectNode that will be merged into.
   * @param updateNode The ObjectNode to merge.
   */
  private static void merge(ObjectNode mainNode, ObjectNode updateNode) {
    updateNode
        .fields()
        .forEachRemaining(
            entry -> {
              String fieldName = entry.getKey();
              JsonNode value = entry.getValue();

              if (!mainNode.has(fieldName)) {
                mainNode.set(fieldName, value);
                return;
              }

              // If the field already exists and both are objects, recursively merge
              JsonNode existingValue = mainNode.get(fieldName);
              if (existingValue.isObject() && value.isObject()) {
                merge((ObjectNode) existingValue, (ObjectNode) value);
                return;
              }

              mainNode.set(fieldName, value);
            });
  }

  public static boolean OpenLineageEnvVarsExist() {
    return Environment.getAllEnvironmentVariables().entrySet().stream()
        .anyMatch(entry -> entry.getKey().startsWith(PREFIX));
  }
}
