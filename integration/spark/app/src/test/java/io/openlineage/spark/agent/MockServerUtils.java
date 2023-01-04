/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static java.nio.file.Files.readAllBytes;
import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.JsonBody.json;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.mockserver.client.MockServerClient;
import org.mockserver.matchers.MatchType;
import org.mockserver.model.JsonBody;
import org.mockserver.model.RequestDefinition;

public class MockServerUtils {

  static void verifyEvents(MockServerClient mockServerClient, String... eventFiles) {
    Path eventFolder = Paths.get("integrations/container/");
    mockServerClient.verify(
        Arrays.stream(eventFiles)
            .map(
                fileEvent ->
                    request()
                        .withPath("/api/v1/lineage")
                        .withBody(readJson(eventFolder.resolve(fileEvent))))
            .collect(Collectors.toList())
            .toArray(new RequestDefinition[0]));
  }

  /**
   * Waits for the mockserver to get COMPLETE event for a given job name. Before verifying requests
   * we need to make sure that requests have been sent to mock server.
   *
   * @param jobName
   */
  static void waitForJobComplete(MockServerClient mockServerClient, String jobName) {
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              mockServerClient.verify(
                  request()
                      .withPath("/api/v1/lineage")
                      .withBody(
                          json(
                              "{\"eventType\": \"COMPLETE\", \"job\": {\"name\": \""
                                  + jobName
                                  + "\"}}",
                              MatchType.ONLY_MATCHING_FIELDS)));
            });
  }

  @SneakyThrows
  static JsonBody readJson(Path path) {
    return json(new String(readAllBytes(path)), MatchType.ONLY_MATCHING_FIELDS);
  }
}
