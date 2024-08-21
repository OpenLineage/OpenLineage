/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static io.openlineage.client.Events.datasetEvent;
import static io.openlineage.client.Events.jobEvent;
import static io.openlineage.client.Events.runEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.datazone.DataZoneClient;
import software.amazon.awssdk.services.datazone.model.InternalServerException;
import software.amazon.awssdk.services.datazone.model.PostLineageEventRequest;
import software.amazon.awssdk.services.datazone.model.PostLineageEventResponse;

class AmazonDataZoneTransportTest {
  private static final String DATAZONE_DOMAIN_ID = "dzd_a1b2c3d4e5f6g7";

  private final DataZoneClient dataZoneClient = mock(DataZoneClient.class);
  private OpenLineageClient openLineageClient;

  @BeforeEach
  public void beforeEach() {
    AmazonDataZoneConfig config = new AmazonDataZoneConfig();
    config.setDomainId(DATAZONE_DOMAIN_ID);
    AmazonDataZoneTransport dataZoneTransport = new AmazonDataZoneTransport(dataZoneClient, config);
    openLineageClient = new OpenLineageClient(dataZoneTransport);
  }

  @Test
  void dataZoneTransportRaisesExceptionWhenDomainIdNotProvided() {
    AmazonDataZoneConfig config = new AmazonDataZoneConfig();

    assertThrows(OpenLineageClientException.class, () -> new AmazonDataZoneTransport(config));
  }

  @Test
  void clientEmitsRunEventDataZoneTransport() {
    when(dataZoneClient.postLineageEvent(any(PostLineageEventRequest.class)))
        .thenReturn(mock(PostLineageEventResponse.class));
    ArgumentCaptor<PostLineageEventRequest> captor =
        ArgumentCaptor.forClass(PostLineageEventRequest.class);
    OpenLineage.RunEvent runEvent = runEvent();

    openLineageClient.emit(runEvent);

    verify(dataZoneClient, times(1)).postLineageEvent(captor.capture());
    assertEquals(DATAZONE_DOMAIN_ID, captor.getValue().domainIdentifier());
    assertNotNull(captor.getValue().event());
  }

  @Test
  void dataZoneTransportRaisesExceptionOnDataZoneInvocationFailure() {
    when(dataZoneClient.postLineageEvent(any(PostLineageEventRequest.class)))
        .thenThrow(InternalServerException.class);
    OpenLineage.RunEvent runEvent = runEvent();

    assertThrows(OpenLineageClientException.class, () -> openLineageClient.emit(runEvent));
  }

  @Test
  void clientEmitsDataSetEventDataZoneTransport() {
    when(dataZoneClient.postLineageEvent(any(PostLineageEventRequest.class)))
        .thenReturn(mock(PostLineageEventResponse.class));
    ArgumentCaptor<PostLineageEventRequest> captor =
        ArgumentCaptor.forClass(PostLineageEventRequest.class);
    OpenLineage.DatasetEvent datasetEvent = datasetEvent();

    openLineageClient.emit(datasetEvent);

    verify(dataZoneClient, times(0)).postLineageEvent(captor.capture());
  }

  @Test
  void dataZoneTransportEmitsJobEvent() {
    when(dataZoneClient.postLineageEvent(any(PostLineageEventRequest.class)))
        .thenReturn(mock(PostLineageEventResponse.class));
    ArgumentCaptor<PostLineageEventRequest> captor =
        ArgumentCaptor.forClass(PostLineageEventRequest.class);
    OpenLineage.JobEvent jobEvent = jobEvent();

    openLineageClient.emit(jobEvent);

    verify(dataZoneClient, times(0)).postLineageEvent(captor.capture());
  }
}
