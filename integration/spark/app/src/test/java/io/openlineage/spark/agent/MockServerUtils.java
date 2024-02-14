/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static java.nio.file.Files.readAllBytes;
import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.JsonBody.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.MatchType;
import org.mockserver.model.JsonBody;
import org.mockserver.model.RequestDefinition;
import org.skyscreamer.jsonassert.JSONAssert;

@Slf4j
public class MockServerUtils {
  private static final ObjectReader READER = new ObjectMapper().findAndRegisterModules().reader();

  static void verifyEvents(Path filePath, String... eventFiles) {
    verifyEvents(filePath, ignored -> true, Collections.emptyMap(), eventFiles);
  }

  static void verifyEvents(Path filePath, Predicate<JsonNode> eventFilter, String... eventFiles) {
    verifyEvents(filePath, eventFilter, Collections.emptyMap(), eventFiles);
  }

  static void verifyEvents(Path filePath, Map<String, String> replacements, String... eventFiles) {
    verifyEvents(filePath, ignored -> true, replacements, eventFiles);
  }

  @SneakyThrows
  static List<OpenLineage.RunEvent> getEmittedEvents(Path filePath) {
    List<String> lines = Files.readAllLines(filePath);
    return lines.stream()
        .map(OpenLineageClientUtils::runEventFromJson)
        .collect(Collectors.toList());
  }

  @SneakyThrows
  static void verifyEvents(
      Path filePath,
      Predicate<JsonNode> eventFilter,
      Map<String, String> replacements,
      String... expectedEventFileNames) {
    Path eventsDir = Paths.get("integrations/container/");

    final List<JsonNode> actualJsonObjects =
        Files.readAllLines(filePath).stream()
            .map(MockServerUtils::sneakyRead)
            .filter(eventFilter)
            .collect(Collectors.toList());

    final List<JsonNode> expectedJsonObjects =
        Arrays.stream(expectedEventFileNames)
            .map(eventsDir::resolve)
            .map(MockServerUtils::sneakyRead)
            .map(json -> doReplace(json, replacements))
            .map(MockServerUtils::sneakyRead)
            .collect(Collectors.toList());

    IntStream.range(0, expectedJsonObjects.size())
        .forEach(
            index -> {
              JsonNode actualJson = actualJsonObjects.get(index);
              JsonNode expectedJson = expectedJsonObjects.get(index);
              try {
                log.info("Comparing {} to {}", expectedJson, actualJson);
                JSONAssert.assertEquals(expectedJson.toString(), actualJson.toString(), false);
              } catch (JSONException e) {
                Assertions.fail("Failed to match JSON", e);
              }
            });
  }

  @SneakyThrows
  private static JsonNode sneakyRead(String json) {
    return MockServerUtils.READER.readTree(json);
  }

  @SneakyThrows
  private static String sneakyRead(Path path) {
    return new String(Files.readAllBytes(path));
  }

  private static String doReplace(String s, Map<String, String> replacements) {
    String result = s;
    for (Map.Entry<String, String> entry : replacements.entrySet()) {
      result = result.replace(entry.getKey(), entry.getValue());
    }
    return result;
  }

  static void verifyEvents(MockServerClient mockServerClient, String... eventFiles) {
    verifyEvents(mockServerClient, Collections.emptyMap(), eventFiles);
  }

  /**
   * @param mockServerClient
   * @param replacements map of string replacements within the json files. Allows injecting spark
   *     version into json files.
   * @param eventFiles
   */
  static void verifyEvents(
      MockServerClient mockServerClient, Map<String, String> replacements, String... eventFiles) {
    Path eventFolder = Paths.get("integrations/container/");

    await()
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () ->
                mockServerClient.verify(
                    Arrays.stream(eventFiles)
                        .map(
                            fileEvent ->
                                request()
                                    .withPath("/api/v1/lineage")
                                    .withBody(
                                        readJson(eventFolder.resolve(fileEvent), replacements)))
                        .collect(Collectors.toList())
                        .toArray(new RequestDefinition[0])));
  }

  @SneakyThrows
  static JsonBody readJson(Path path, Map<String, String> replacements) {
    final String[] fileContent = {new String(readAllBytes(path))};
    replacements.forEach(
        (find, replace) -> {
          fileContent[0] = fileContent[0].replace(find, replace);
        });

    return json(fileContent[0], MatchType.ONLY_MATCHING_FIELDS);
  }

  public static List<RunEvent> getEventsEmittedWithJobName(
      ClientAndServer mockServer, String jobNameContains) {

    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .until(
            () ->
                Arrays.stream(
                        mockServer.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
                    .map(r -> OpenLineageClientUtils.runEventFromJson(r.getBodyAsString()))
                    .anyMatch(
                        e ->
                            e.getJob().getName().contains(jobNameContains)
                                && e.getEventType().equals(RunEvent.EventType.COMPLETE)));

    return Arrays.stream(mockServer.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
        .map(r -> OpenLineageClientUtils.runEventFromJson(r.getBodyAsString()))
        .filter(e -> e.getJob().getName().contains(jobNameContains))
        .collect(Collectors.toList());
  }

  public static List<RunEvent> getEventsEmitted(ClientAndServer mockServer) {
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .until(
            () ->
                Arrays.stream(
                        mockServer.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
                    .map(r -> OpenLineageClientUtils.runEventFromJson(r.getBodyAsString()))
                    .findAny()
                    .isPresent());

    return Arrays.stream(mockServer.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
        .map(r -> OpenLineageClientUtils.runEventFromJson(r.getBodyAsString()))
        .collect(Collectors.toList());
  }
}
