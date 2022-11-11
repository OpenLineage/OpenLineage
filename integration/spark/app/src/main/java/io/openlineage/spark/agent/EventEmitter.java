/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.ApiKeyTokenProvider;
import io.openlineage.client.transports.ConsoleTransport;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;
import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportFactory;
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
  @Getter private String jobNamespace;
  @Getter private String parentJobName;
  @Getter private Optional<UUID> parentRunId;

  public EventEmitter(
      Transport transport, String jobNamespace, String parentJobName, Optional<UUID> parentRunId) {
    this.client = new OpenLineageClient(transport);
    this.jobNamespace = jobNamespace;
    this.parentJobName = parentJobName;
    this.parentRunId = parentRunId;
  }

  /**
   * @param argument
   * @throws URISyntaxException
   * @deprecated Use {@link EventEmitter#EventEmitter(Transport, String, String, Optional)}
   */
  @Deprecated
  public EventEmitter(ArgumentParser argument) throws URISyntaxException {
    this.jobNamespace = argument.getNamespace();
    this.parentJobName = argument.getJobName();
    this.parentRunId = convertToUUID(argument.getParentRunId());

    if (argument.isConsoleMode()) {
      this.client = new OpenLineageClient(new ConsoleTransport());
      log.info("Init OpenLineageContext: will output events to console");
      return;
    }

    if (argument.getTransportConfig().isPresent()) {
      this.client =
          new OpenLineageClient(new TransportFactory(argument.getTransportConfig().get()).build());
      log.info(
          String.format(
              "Init OpenLineageContext: use %s as transport, with config %s",
              argument.getTransportMode().get(), argument.getTransportConfig().get()));
      return;
    }

    HttpConfig config = buildHttpConfig(argument);

    this.client = OpenLineageClient.builder().transport(new HttpTransport(config)).build();
    log.debug(
        String.format("Init OpenLineageContext: Args: %s URI: %s", argument, config.getUrl()));
  }

  static HttpConfig buildHttpConfig(ArgumentParser argument) throws URISyntaxException {
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

    URI lineageURI =
        new URI(hostURI.getScheme(), hostURI.getAuthority(), uriPath, queryParams, null);

    HttpConfig config = new HttpConfig();
    argument.getApiKey().ifPresent(key -> config.setAuth(new ApiKeyTokenProvider(key)));
    argument.getTimeout().ifPresent(config::setTimeout);
    config.setUrl(lineageURI);
    return config;
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
