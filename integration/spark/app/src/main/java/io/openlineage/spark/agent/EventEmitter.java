/*
/* Copyright 2018-2023 contributors to the OpenLineage project
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class EventEmitter {
  @Getter private OpenLineageClient client;
  @Getter private Optional<String> appName;
  @Getter private String jobNamespace;
  @Getter private String parentJobName;
  @Getter private Optional<UUID> parentRunId;
  @Getter private Optional<List<String>> customEnvironmentVariables;

  public EventEmitter(ArgumentParser argument) throws URISyntaxException {
    this.jobNamespace = argument.getNamespace();
    this.parentJobName = argument.getJobName();
    this.parentRunId = convertToUUID(argument.getParentRunId());
    this.appName = Optional.ofNullable(argument.getAppName());
    this.customEnvironmentVariables = Optional.of(Arrays.asList(argument.getOpenLineageYaml().getFacetsConfig().getCustomEnvironmentVariables()));
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
