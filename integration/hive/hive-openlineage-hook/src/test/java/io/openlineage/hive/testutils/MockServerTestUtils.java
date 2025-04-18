/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.testutils;

import static java.nio.file.Files.readAllBytes;
import static org.awaitility.Awaitility.await;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.JsonBody.json;

import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.mockserver.client.MockServerClient;
import org.mockserver.matchers.MatchType;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.JsonBody;
import org.mockserver.model.RequestDefinition;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class MockServerTestUtils {

  public static final DockerImageName MOCKSERVER_IMAGE =
      DockerImageName.parse("mockserver/mockserver")
          .withTag("mockserver-" + MockServerClient.class.getPackage().getImplementationVersion());

  public static final HttpRequest API_V1_LINEAGE_REQUEST = request("/api/v1/lineage");

  public static MockServerContainer makeMockServerContainer(Network network) {
    return new MockServerContainer(MOCKSERVER_IMAGE)
        .withNetwork(network)
        .withNetworkAliases("openlineageendpoint")
        .withStartupTimeout(Duration.of(2, ChronoUnit.MINUTES));
  }

  public static void verifyEvents(MockServerClient mockServerClient, String... eventFiles) {
    verifyEvents(mockServerClient, Collections.emptyMap(), eventFiles);
  }

  /**
   * @param mockServerClient
   * @param replacements map of string replacements within the json files. Allows injecting spark
   *     version into json files.
   * @param eventFiles
   */
  public static void verifyEvents(
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

  public static List<RunEvent> getEventsEmitted(MockServerClient mockServerClient) {
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .until(
            () ->
                Arrays.stream(
                        mockServerClient.retrieveRecordedRequests(
                            request().withPath("/api/v1/lineage")))
                    .map(r -> OpenLineageClientUtils.runEventFromJson(r.getBodyAsString()))
                    .findAny()
                    .isPresent());

    return Arrays.stream(
            mockServerClient.retrieveRecordedRequests(request().withPath("/api/v1/lineage")))
        .map(r -> OpenLineageClientUtils.runEventFromJson(r.getBodyAsString()))
        .collect(Collectors.toList());
  }
}
