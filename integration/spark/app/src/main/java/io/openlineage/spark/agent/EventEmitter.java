/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.TransportFactory;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventEmitter {
  @Getter private OpenLineageClient client;
  @Getter private Optional<String> overriddenAppName;
  @Getter private String jobNamespace;
  @Getter private Optional<String> parentJobName;
  @Getter private Optional<String> parentJobNamespace;
  @Getter private Optional<UUID> parentRunId;
  @Getter private UUID applicationRunId;
  @Getter private String applicationJobName;
  @Getter private Optional<List<String>> customEnvironmentVariables;

  public EventEmitter(SparkOpenLineageConfig config, String applicationJobName)
      throws URISyntaxException {
    this.jobNamespace = config.getNamespace();
    this.parentJobName = Optional.ofNullable(config.getParentJobName());
    this.parentJobNamespace = Optional.ofNullable(config.getParentJobNamespace());
    this.parentRunId = convertToUUID(config.getParentRunId());
    this.overriddenAppName = Optional.ofNullable(config.getOverriddenAppName());
    this.customEnvironmentVariables =
        config.getFacetsConfig() != null
            ? config.getFacetsConfig().getCustomEnvironmentVariables() != null
                ? Optional.of(
                    Arrays.asList(config.getFacetsConfig().getCustomEnvironmentVariables()))
                : Optional.empty()
            : Optional.empty();
    String[] disabledFacets = config.getFacetsConfig().getDisabledFacets();
    this.client =
        OpenLineageClient.builder()
            .transport(new TransportFactory(config.getTransportConfig()).build())
            .disableFacets(disabledFacets)
            .build();
    this.applicationJobName = applicationJobName;
    this.applicationRunId = UUID.randomUUID();
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
