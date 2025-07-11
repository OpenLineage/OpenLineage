/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.datazone;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.Transport;
import java.net.URI;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.datazone.DataZoneClient;
import software.amazon.awssdk.services.datazone.DataZoneClientBuilder;
import software.amazon.awssdk.services.datazone.model.PostLineageEventRequest;
import software.amazon.awssdk.services.datazone.model.PostLineageEventResponse;
import software.amazon.awssdk.utils.StringUtils;

@Slf4j
public final class AmazonDataZoneTransport extends Transport {
  private final DataZoneClient dataZoneClient;
  private final String domainId;

  public AmazonDataZoneTransport(@NonNull final AmazonDataZoneConfig dataZoneConfig) {
    this(buildDataZoneClient(dataZoneConfig), dataZoneConfig);
  }

  public AmazonDataZoneTransport(
      @NonNull final DataZoneClient dataZoneClient,
      @NonNull final AmazonDataZoneConfig dataZoneConfig) {
    validateDataZoneConfig(dataZoneConfig);
    this.dataZoneClient = dataZoneClient;
    this.domainId = dataZoneConfig.getDomainId();
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    emit(OpenLineageClientUtils.toJson(runEvent));
  }

  @Override
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    // Not supported in DataZone
    log.debug(
        "DatasetEvent is not supported in DataZone {}",
        OpenLineageClientUtils.toJson(datasetEvent));
  }

  @Override
  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
    // Not supported in DataZone
    log.debug("JobEvent is not supported in DataZone {}", OpenLineageClientUtils.toJson(jobEvent));
  }

  public void emit(String eventAsJson) {
    try {
      PostLineageEventResponse response =
          this.dataZoneClient.postLineageEvent(
              PostLineageEventRequest.builder()
                  .domainIdentifier(this.domainId)
                  .event(SdkBytes.fromUtf8String(eventAsJson))
                  .build());
      log.info(
          "Successfully posted a LineageEvent: {} in Domain: {}",
          response.id(),
          response.domainId());
    } catch (Exception e) {
      throw new OpenLineageClientException(
          String.format("Failed to send lineage event to DataZone: %s", eventAsJson), e);
    }
  }

  @Override
  public void close() throws Exception {
    this.dataZoneClient.close();
  }

  private void validateDataZoneConfig(@NonNull final AmazonDataZoneConfig dataZoneConfig) {
    if (dataZoneConfig.getDomainId() == null) {
      throw new OpenLineageClientException(
          "DomainId can't be null, try setting transport.domainId in config");
    }
  }

  private static DataZoneClient buildDataZoneClient(AmazonDataZoneConfig config) {
    DataZoneClientBuilder builder =
        DataZoneClient.builder()
            .httpClientBuilder(ApacheHttpClient.builder())
            .credentialsProvider(DefaultCredentialsProvider.create());
    if (!StringUtils.isBlank(config.getEndpointOverride())) {
      builder.endpointOverride(URI.create(config.getEndpointOverride()));
    }

    return builder.build();
  }
}
