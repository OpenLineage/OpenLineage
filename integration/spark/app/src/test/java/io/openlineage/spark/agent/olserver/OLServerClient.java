/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.olserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.spark.agent.util.MatchesMapRecursively;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

@Slf4j
public class OLServerClient {
  private String host;
  private int port;
  private ObjectMapper objectMapper = new ObjectMapper();

  public OLServerClient(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public void verifyEvents(String... eventFiles) {
    verifyEvents(Collections.emptyMap(), eventFiles);
  }

  /**
   * @param replacements map of string replacements within the json files. Allows injecting spark
   *     version into json files.
   * @param eventFiles
   */
  @SneakyThrows
  public void verifyEvents(Map<String, String> replacements, String... eventFiles) {
    Path eventFolder = Paths.get("integrations/container/");

    List<Map<String, Object>> nodes =
        Arrays.stream(eventFiles)
            .map(eventFolder::resolve)
            .map(this::readJson)
            .collect(Collectors.toList());

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              List<Boolean> bln =
                  nodes.stream()
                      .map(this::getEvent)
                      .map(Optional::isPresent)
                      .collect(Collectors.toList());
              assertThat(bln.size()).isEqualTo(nodes.size());
            });
    nodes.stream()
        .map(this::getEvent)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(
            event -> {
              String eventJobName = (String) ((Map<?, ?>) event.get("job")).get("name");
              Optional<Map<String, Object>> node =
                  nodes.stream()
                      .filter(
                          n ->
                              n.get("job") instanceof Map
                                  && ((Map<?, ?>) n.get("job")).get("name").equals(eventJobName))
                      .findFirst();
              if (node.isPresent()) {
                assertThat(event)
                    .satisfies(new MatchesMapRecursively(node.get(), Collections.emptySet()));
              } else {
                throw new AssertionError("Event not found: " + eventJobName);
              }
            });
  }

  @SneakyThrows
  public Optional<Map<String, Object>> getEvent(Map<String, Object> event) {
    Object job = event.get("job");
    if (job instanceof Map) {
      Object name = ((Map<?, ?>) job).get("name");
      if (name instanceof String) {
        return getEvent((String) name);
      }
    }
    return Optional.empty();
  }

  @SneakyThrows
  public Optional<Map<String, Object>> getEvent(String jobName) {
    final HttpPost httpPost =
        new HttpPost(new URI("http", null, host, port, "api/v1/lineage", null, null));
    final List<NameValuePair> params = new ArrayList<NameValuePair>();
    params.add(new BasicNameValuePair("job_name", jobName));
    httpPost.setEntity(new UrlEncodedFormEntity(params));

    httpPost.setHeader("Accept", "application/json");
    httpPost.setHeader("Content-type", "application/json");

    try (CloseableHttpClient client = HttpClients.createDefault()) {
      try (CloseableHttpResponse response = client.execute(httpPost)) {
        log.error("GOT {}", response.getStatusLine().getStatusCode());
        if (response.getStatusLine().getStatusCode() >= 300) {
          return Optional.empty();
        }
        Map<String, Object> runEvent =
            objectMapper.readValue(response.getEntity().getContent(), Map.class);
        log.error("EVENT {}", objectMapper.writeValueAsString(runEvent));
        return Optional.of(runEvent);
      }
    }
  }

  @SneakyThrows
  private Map<String, Object> readJson(Path path) {
    return objectMapper.readValue(path.toFile(), Map.class);
  }
}
