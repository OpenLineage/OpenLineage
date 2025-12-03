/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static java.nio.file.Files.readAllBytes;
import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;

import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.mockserver.client.MockServerClient;
import org.mockserver.configuration.Configuration;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.MatchType;
import org.mockserver.model.ClearType;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.JsonBody;
import org.mockserver.model.RequestDefinition;
import org.slf4j.event.Level;

@Slf4j
public class MockServerUtils {
  public static final HttpRequest API_V1_LINEAGE_REQUEST = request("/api/v1/lineage");

  /**
   * This will create and configure a MockServer instance with the standard expectation for the
   * OpenLineage API. It will respond with a 201 status code for any request to the /api/v1/lineage
   * endpoint.
   *
   * @param mockServerPort the port that the MockServer should listen on
   * @return the instance of the MockServer that was created and configured
   */
  public static ClientAndServer createAndConfigureMockServer(int mockServerPort) {
    Configuration config = Configuration.configuration();
    config.logLevel(Level.ERROR);
    ClientAndServer mockServer = ClientAndServer.startClientAndServer(config, mockServerPort);
    configureStandardExpectation(mockServer);
    return mockServer;
  }

  /**
   * This will configure the standard expectation for the OpenLineage API. It will respond with a
   * 201 status code.
   *
   * @param mockServer the instance that should have its expectations configured
   */
  public static void configureStandardExpectation(ClientAndServer mockServer) {
    mockServer.when(API_V1_LINEAGE_REQUEST).respond(response().withStatusCode(201));
  }

  /**
   * This wil reset ONLY the logs (requests) that were made to MockServer. It will still retain the
   * expectations.
   *
   * @param mockServer the instance that should have its logs cleared
   */
  public static void clearRequests(ClientAndServer mockServer) {
    mockServer.clear(API_V1_LINEAGE_REQUEST, ClearType.LOG);
  }

  /**
   * This will reset the MockServer instance, clearing all expectations and logs. It will also stop
   * it.
   *
   * @param mockServer the instance that should be reset and stopped
   */
  public static void stopMockServer(ClientAndServer mockServer) {
    mockServer.reset();
    mockServer.stop();
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
        .atMost(Duration.ofSeconds(30))
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

  public static List<RunEvent> getEventsEmitted(MockServerClient mockServer) {
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
