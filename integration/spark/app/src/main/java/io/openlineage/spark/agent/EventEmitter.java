/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.FacetsConfig;
import io.openlineage.client.transports.TransportFactory;
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

  public EventEmitter(ArgumentParser argument, String applicationJobName)
      throws URISyntaxException {
    this.jobNamespace = argument.getNamespace();
    this.parentJobName = Optional.ofNullable(argument.getParentJobName());
    this.parentJobNamespace = Optional.ofNullable(argument.getParentJobNamespace());
    this.parentRunId = convertToUUID(argument.getParentRunId());
    this.overriddenAppName = Optional.ofNullable(argument.getOverriddenAppName());
    this.customEnvironmentVariables =
        argument.getOpenLineageYaml().getFacetsConfig() != null
            ? argument.getOpenLineageYaml().getFacetsConfig().getCustomEnvironmentVariables()
                    != null
                ? Optional.of(
                    Arrays.asList(
                        argument
                            .getOpenLineageYaml()
                            .getFacetsConfig()
                            .getCustomEnvironmentVariables()))
                : Optional.empty()
            : Optional.empty();
    String[] disabledFacets =
        Optional.ofNullable(argument.getOpenLineageYaml().getFacetsConfig())
            .orElse(new FacetsConfig().withDisabledFacets(new String[0]))
            .getDisabledFacets();
    this.client =
        OpenLineageClient.builder()
            .transport(
                new TransportFactory(argument.getOpenLineageYaml().getTransportConfig()).build())
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
