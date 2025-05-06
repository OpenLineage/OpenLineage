/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.datazone;

import static io.openlineage.client.testdata.OpenLineageEventsDataHelper.datasetEvent;
import static io.openlineage.client.testdata.OpenLineageEventsDataHelper.jobEvent;
import static io.openlineage.client.testdata.OpenLineageEventsDataHelper.runEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_ACCESS_KEY_ID;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_REGION;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_SECRET_ACCESS_KEY;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_SESSION_TOKEN;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.transports.TransportFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.datazone.DataZoneClient;
import software.amazon.awssdk.services.datazone.model.InternalServerException;
import software.amazon.awssdk.services.datazone.model.PostLineageEventRequest;
import software.amazon.awssdk.services.datazone.model.PostLineageEventResponse;

class AmazonDataZoneTransportTests {
  private static final String DATAZONE_DOMAIN_ID = "dzd_a1b2c3d4e5f6g7";

  private final DataZoneClient dataZoneClient = mock(DataZoneClient.class);
  private OpenLineageClient openLineageClient;

  @BeforeEach
  public void beforeEach() {
    System.setProperty(AWS_REGION.property(), Region.EU_WEST_1.id());
    System.setProperty(AWS_ACCESS_KEY_ID.property(), "AccessKey");
    System.setProperty(AWS_SECRET_ACCESS_KEY.property(), "SecretAccessKey");
    System.setProperty(AWS_SESSION_TOKEN.property(), "AWSSessionToken");

    AmazonDataZoneConfig config = new AmazonDataZoneConfig();
    config.setDomainId(DATAZONE_DOMAIN_ID);
    AmazonDataZoneTransport dataZoneTransport = new AmazonDataZoneTransport(dataZoneClient, config);
    openLineageClient = new OpenLineageClient(dataZoneTransport);
  }

  @AfterEach
  void clearAWSSystemProperties() {
    System.clearProperty(AWS_REGION.property());
    System.clearProperty(AWS_ACCESS_KEY_ID.property());
    System.clearProperty(AWS_SECRET_ACCESS_KEY.property());
    System.clearProperty(AWS_SESSION_TOKEN.property());
  }

  @Test
  void transportFactoryCreatesAmazonDataZoneTransport() {
    AmazonDataZoneConfig config = new AmazonDataZoneConfig();
    config.setDomainId(DATAZONE_DOMAIN_ID);
    TransportFactory transportFactory = new TransportFactory(config);

    assertTrue(transportFactory.build() instanceof AmazonDataZoneTransport);
  }

  @Test
  void transportFactoryCreatesAmazonDataZoneTransportWithEndpointOverride() {
    AmazonDataZoneConfig config = new AmazonDataZoneConfig();
    config.setDomainId(DATAZONE_DOMAIN_ID);
    config.setEndpointOverride("https://datazone.us-east-1.api.aws");
    TransportFactory transportFactory = new TransportFactory(config);

    assertTrue(transportFactory.build() instanceof AmazonDataZoneTransport);
  }

  @Test
  void dataZoneTransportRaisesExceptionWhenDomainIdNotProvided() {
    AmazonDataZoneConfig config = new AmazonDataZoneConfig();

    assertThrows(
        OpenLineageClientException.class,
        () -> new AmazonDataZoneTransport(dataZoneClient, config));
  }

  @Test
  void dataZoneTransportWithoutClientRaisesExceptionWhenDomainIdNotProvided() {
    AmazonDataZoneConfig config = new AmazonDataZoneConfig();

    assertThrows(OpenLineageClientException.class, () -> new AmazonDataZoneTransport(config));
  }

  @Test
  void dataZoneTransportRaisesExceptionOnDataZoneInvocationFailure() {
    when(dataZoneClient.postLineageEvent(any(PostLineageEventRequest.class)))
        .thenThrow(InternalServerException.class);
    OpenLineage.RunEvent runEvent = runEvent();

    assertThrows(OpenLineageClientException.class, () -> openLineageClient.emit(runEvent));
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
  void clientEmitsJobEventDataZoneTransport() {
    when(dataZoneClient.postLineageEvent(any(PostLineageEventRequest.class)))
        .thenReturn(mock(PostLineageEventResponse.class));
    ArgumentCaptor<PostLineageEventRequest> captor =
        ArgumentCaptor.forClass(PostLineageEventRequest.class);
    OpenLineage.JobEvent jobEvent = jobEvent();

    openLineageClient.emit(jobEvent);

    verify(dataZoneClient, times(0)).postLineageEvent(captor.capture());
  }
}
