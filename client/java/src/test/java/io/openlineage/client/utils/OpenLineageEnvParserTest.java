/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.Environment;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.transports.FileConfig;
import io.openlineage.client.transports.HttpConfig;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
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

  @ParameterizedTest
  @MethodSource("valueArgs")
  void testParseValue(String envValue, String expectedJson) throws Exception {
    assertEquals(
        new ObjectMapper().readTree(expectedJson), OpenLineageEnvParser.parseValue(envValue));
  }

  private static Stream<Arguments> valueArgs() {
    return Stream.of(
        Arguments.of("true", "true"),
        Arguments.of("null", "null"),
        Arguments.of("30", "30"),
        Arguments.of("0.2", "0.2"),
        Arguments.of("1.0", "1.0"),
        Arguments.of("{\"a\": 1}", "{\"a\": 1}"),
        Arguments.of("[1, 2]", "[1, 2]"),
        Arguments.of("PRODUCTION", "\"PRODUCTION\""),
        Arguments.of("", "\"\""),
        // numbers whose JSON text does not round-trip are kept as written
        Arguments.of("1.10", "\"1.10\""),
        Arguments.of("1e5", "\"1e5\""),
        Arguments.of("1234e5678", "\"1234e5678\""),
        // text after the first JSON token is kept as written
        Arguments.of("true;env:prod", "\"true;env:prod\""),
        Arguments.of("false-abc def", "\"false-abc def\""),
        Arguments.of("null x", "\"null x\""),
        Arguments.of("123 456", "\"123 456\""),
        Arguments.of("{\"a\": 1} x", "\"{\\\"a\\\": 1} x\""));
  }

  @ParameterizedTest
  @ValueSource(strings = {"1234e5678", "1.10", "12345678", "1.0"})
  void testApiKeyKeepsEnvText(String apiKey) throws Exception {
    OpenLineageConfig config =
        loadConfig(
            Map.of(
                "OPENLINEAGE__TRANSPORT__TYPE", "http",
                "OPENLINEAGE__TRANSPORT__URL", "http://localhost:5000",
                "OPENLINEAGE__TRANSPORT__AUTH__TYPE", "api_key",
                "OPENLINEAGE__TRANSPORT__AUTH__API_KEY", apiKey));
    HttpConfig httpConfig = (HttpConfig) config.getTransportConfig();
    assertEquals("Bearer " + apiKey, httpConfig.getAuth().getToken());
  }

  @Test
  void testNumericSettingStillParsed() throws Exception {
    OpenLineageConfig config =
        loadConfig(
            Map.of(
                "OPENLINEAGE__TRANSPORT__TYPE", "http",
                "OPENLINEAGE__TRANSPORT__URL", "http://localhost:5000",
                "OPENLINEAGE__TRANSPORT__TIMEOUT_IN_MILLIS", "5000"));
    assertEquals(5000, ((HttpConfig) config.getTransportConfig()).getTimeoutInMillis());
  }

  @Test
  void testFileLocationKeepsEnvText() throws Exception {
    OpenLineageConfig config =
        loadConfig(
            Map.of(
                "OPENLINEAGE__TRANSPORT__TYPE", "file",
                "OPENLINEAGE__TRANSPORT__LOCATION", "20261005"));
    assertEquals("20261005", ((FileConfig) config.getTransportConfig()).getLocation());
  }

  @ParameterizedTest
  @MethodSource("tagArgs")
  void testJobTagsFromEnv(String envValue, List<TagField> expected) throws Exception {
    OpenLineageConfig config =
        loadConfig(
            Map.of("OPENLINEAGE__TRANSPORT__TYPE", "console", "OPENLINEAGE__JOB__TAGS", envValue));
    assertEquals(expected, config.getJobConfig().getTags());
  }

  private static Stream<Arguments> tagArgs() {
    return Stream.of(
        Arguments.of(
            "adhoc:true;version:1.10;ticket:4521",
            List.of(
                new TagField("adhoc", "true", "CONFIG"),
                new TagField("version", "1.10", "CONFIG"),
                new TagField("ticket", "4521", "CONFIG"))),
        // a single label that is valid JSON used to be dropped
        Arguments.of("4521", List.of(new TagField("4521", "true", "CONFIG"))),
        Arguments.of("true", List.of(new TagField("true", "true", "CONFIG"))),
        Arguments.of("1.10", List.of(new TagField("1.10", "true", "CONFIG"))),
        // starts with a JSON literal, but is not one
        Arguments.of(
            "true;env:prod",
            List.of(
                new TagField("true", "true", "CONFIG"), new TagField("env", "prod", "CONFIG"))));
  }

  private static OpenLineageConfig loadConfig(Map<String, String> env) {
    try (MockedStatic<Environment> mocked = mockStatic(Environment.class)) {
      when(Environment.getAllEnvironmentVariables()).thenReturn(env);
      return OpenLineageClientUtils.loadOpenLineageConfigFromEnvVars(
          new TypeReference<OpenLineageConfig>() {});
    }
  }
}
