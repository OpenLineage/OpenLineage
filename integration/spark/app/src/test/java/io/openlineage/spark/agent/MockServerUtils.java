/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static java.nio.file.Files.readAllBytes;
import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;
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
import org.awaitility.Awaitility;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.MatchType;
import org.mockserver.model.JsonBody;
import org.mockserver.model.RequestDefinition;

public class MockServerUtils {

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
        .atMost(Duration.ofSeconds(20))
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
