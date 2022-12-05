/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.google.common.util.concurrent.ListenableFuture;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class KinesisTransportTest {
  @Test
  void clientEmitsKinesisTransport() throws IOException {
    KinesisProducer producer = mock(KinesisProducer.class);
    KinesisConfig config = new KinesisConfig();

    Properties properties = new Properties();
    properties.setProperty("MinConnections", "1");

    config.setRegion("us-west-2");
    config.setRoleArn("test-role");
    config.setStreamName("test-stream");
    config.setProperties(properties);

    KinesisTransport transport = new KinesisTransport(producer, config);
    OpenLineageClient client = new OpenLineageClient(transport);

    when(producer.addUserRecord(any(UserRecord.class))).thenReturn(mock(ListenableFuture.class));

    client.emit(
        new OpenLineage(URI.create("http://test.producer"))
            .newRunEventBuilder()
            .job(new OpenLineage.JobBuilder().name("test-job").namespace("test-ns").build())
            .build());

    ArgumentCaptor<UserRecord> captor = ArgumentCaptor.forClass(UserRecord.class);

    verify(producer, times(1)).addUserRecord(captor.capture());

    assertThat(captor.getValue().getStreamName()).isEqualTo("test-stream");
  }
}
