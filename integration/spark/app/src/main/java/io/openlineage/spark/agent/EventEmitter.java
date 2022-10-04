/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.ConsoleTransport;
import io.openlineage.client.transports.HttpTransport;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventEmitter {
  @Getter private OpenLineageClient client;
  @Getter private URI lineageURI;
  @Getter private Optional<String> appName;
  @Getter private String jobNamespace;
  @Getter private String parentJobName;
  @Getter private Double timeout;
  @Getter private Optional<UUID> parentRunId;

  public EventEmitter(ArgumentParser argument) throws URISyntaxException {
    this.jobNamespace = argument.getNamespace();
    this.parentJobName = argument.getJobName();
    this.parentRunId = convertToUUID(argument.getParentRunId());
    this.appName = argument.getAppName();

    if (argument.isConsoleMode()) {
      this.client = new OpenLineageClient(new ConsoleTransport());
      log.info("Init OpenLineageContext: will output events to console");
      return;
    }

    // Extract url parameters other than api_key to append to lineageURI
    String queryParams = null;
    if (argument.getUrlParams().isPresent()) {
      Map<String, String> urlParams = argument.getUrlParams().get();

      StringJoiner query = new StringJoiner("&");
      urlParams.forEach((k, v) -> query.add(k + "=" + v));

      queryParams = query.toString();
    }

    // Convert host to a URI to extract scheme and authority
    URI hostURI = new URI(argument.getHost());
    String uriPath = String.format("/api/%s/lineage", argument.getVersion());

    this.lineageURI =
        new URI(hostURI.getScheme(), hostURI.getAuthority(), uriPath, queryParams, null);

    HttpTransport.Builder builder = HttpTransport.builder().uri(this.lineageURI);
    argument.getApiKey().ifPresent(builder::apiKey);
    argument.getTimeout().ifPresent(builder::timeout);

    this.client = OpenLineageClient.builder().transport(builder.build()).build();
    log.debug(
        String.format(
            "Init OpenLineageContext: Args: %s URI: %s", argument, lineageURI.toString()));
  }

  public void emit(OpenLineage.RunEvent event) {
    try {
      this.client.emit(event);
      log.debug(
          "Emitting lineage completed successfully: {}", OpenLineageClientUtils.toJson(event));
    } catch (OpenLineageClientException exception) {
      log.error("Could not emit lineage w/ exception", exception);
    }
  }

  private static Optional<UUID> convertToUUID(String uuid) {
    try {
      return Optional.ofNullable(uuid).map(UUID::fromString);
    } catch (Exception e) {
      return Optional.empty();
    }
  }
}
