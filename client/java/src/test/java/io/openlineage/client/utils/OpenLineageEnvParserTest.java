/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.Environment;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

class OpenLineageEnvParserTest {

  @ParameterizedTest
  @MethodSource("generateArgs")
  void testParseAllOpenLineageEnvVars(Map<String, String> mocks, String expectedJson)
      throws Exception {
    try (MockedStatic mocked = mockStatic(Environment.class)) {
      when(Environment.getAllEnvironmentVariables()).thenReturn(mocks);

      // Call the method to test
      String result = OpenLineageEnvParser.parseAllOpenLineageEnvVars();

      // Assert that the result matches the expected JSON
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode expectedNode = objectMapper.readTree(expectedJson);
      JsonNode actualNode = objectMapper.readTree(result);
      assertEquals(expectedNode, actualNode, "The parsed JSON structure is incorrect.");
    }
  }

  private static Stream<Arguments> generateArgs() {
    return Stream.of(
        // Empty map
        Arguments.of(Map.of(), "{}"),
        // Variavle with valid JSON
        Arguments.of(
            Map.of(
                "OPENLINEAGE__API__URL", "http://example.com/api",
                "OPENLINEAGE__API__TIMEOUT", "30"),
            "{"
                + "  \"api\": {"
                + "    \"timeout\": 30,"
                + "    \"url\": \"http://example.com/api\""
                + "  }"
                + "}"),
        // Variable with invalid JSON
        Arguments.of(
            Map.of(
                "OPENLINEAGE__API__URL", "http://example.com/api",
                "OPENLINEAGE__API__TIMEOUT", "invalid_json"),
            "{"
                + "  \"api\": {"
                + "    \"url\": \"http://example.com/api\","
                + "    \"timeout\": \"invalid_json\""
                + "  }"
                + "}"),
        // Unmatching variable
        Arguments.of(
            Map.of(
                "OPENLINEAGE__API__URL", "http://example.com/api",
                "SOME_OTHER_ENV", "ignore_this"),
            "{\"api\":{\"url\":\"http://example.com/api\"}}"),
        // Precedence with overlapping variables
        Arguments.of(
            Map.of(
                "OPENLINEAGE__TRANSPORT",
                    "{\"type\": \"http\", \"endpoint\": \"http://example.com/api\"}",
                "OPENLINEAGE__TRANSPORT__TYPE", "console"),
            "{"
                + "   \"transport\":{"
                + "     \"type\":\"http\","
                + "     \"endpoint\":\"http://example.com/api\""
                + "   }"
                + "}"),
        Arguments.of(
            Map.of(
                "OPENLINEAGE__DATASET__NAMESPACE_RESOLVERS__RESOLVED_NAME__TYPE", "hostList",
                "OPENLINEAGE__DATASET__NAMESPACE_RESOLVERS__RESOLVED_NAME__HOSTS",
                    "[\"kafka-prod13.company.com\", \"kafka-prod15.company.com\"]",
                "OPENLINEAGE__DATASET__NAMESPACE_RESOLVERS__RESOLVED_NAME__SCHEMA", "kafka"),
            "{\n"
                + "  \"dataset\":{"
                + "     \"namespaceResolvers\":{"
                + "          \"resolvedName\":{"
                + "             \"type\":\"hostList\","
                + "             \"schema\":\"kafka\","
                + "             \"hosts\":[\"kafka-prod13.company.com\",\"kafka-prod15.company.com\"]"
                + "          }"
                + "     }"
                + "  }"
                + "}"));
  }
}
