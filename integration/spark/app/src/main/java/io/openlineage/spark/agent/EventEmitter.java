/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.TransportFactory;
import io.openlineage.client.utils.UUIDUtils;
import io.openlineage.spark.api.DebugConfig;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventEmitter {
  @Getter private OpenLineageClient client;
  @Getter private String jobNamespace;
  @Getter private Optional<String> parentJobName;
  @Getter private Optional<String> parentJobNamespace;
  @Getter private Optional<UUID> parentRunId;
  @Getter private Optional<String> rootParentJobName;
  @Getter private Optional<String> rootParentJobNamespace;
  @Getter private Optional<UUID> rootParentRunId;
  @Getter private UUID applicationRunId;
  @Getter private String applicationJobName;
  @Getter private Optional<List<String>> customEnvironmentVariables;

  public EventEmitter(SparkOpenLineageConfig config, String applicationJobName)
      throws URISyntaxException {
    this.jobNamespace = config.getNamespace();
    this.parentJobName = Optional.ofNullable(config.getParentJobName());
    this.parentJobNamespace = Optional.ofNullable(config.getParentJobNamespace());
    this.parentRunId = convertToUUID(config.getParentRunId());
    this.rootParentJobName = Optional.ofNullable(config.getRootParentJobName());
    this.rootParentJobNamespace = Optional.ofNullable(config.getRootParentJobNamespace());
    this.rootParentRunId = convertToUUID(config.getRootParentRunId());
    this.customEnvironmentVariables =
        config.getFacetsConfig() != null
            ? config.getFacetsConfig().getCustomEnvironmentVariables() != null
                ? Optional.of(
                    Arrays.asList(config.getFacetsConfig().getCustomEnvironmentVariables()))
                : Optional.empty()
            : Optional.empty();

    List<String> disabledFacets =
        Stream.of(config.getFacetsConfig().getEffectiveDisabledFacets())
            .collect(Collectors.toList());
    // make sure DebugFacet is not disabled if smart debug is enabled
    // debug facet will be only sent when triggered with smart debug. Facet filtering is done
    // on the Spark side, so we can exclude it here
    Optional.ofNullable(config.getDebugConfig())
        .filter(DebugConfig::isSmartEnabled)
        .ifPresent(e -> disabledFacets.remove("debug"));

    this.client =
        OpenLineageClient.builder()
            .transport(new TransportFactory(config.getTransportConfig()).build())
            .disableFacets(disabledFacets.toArray(new String[0]))
            .build();
    this.applicationJobName = applicationJobName;
    this.applicationRunId = UUIDUtils.generateNewUUID();
  }

  public void emit(OpenLineage.RunEvent event) {
    try {
      this.client.emit(event);
      if (log.isDebugEnabled()) {
        log.debug(
            "Emitting lineage completed successfully  with run id: {}: {}",
            event.getRun().getRunId(),
            OpenLineageClientUtils.toJson(event));
      } else {
        log.info(
            "Emitting lineage completed successfully with run id: {}", event.getRun().getRunId());
      }
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
