/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Subscription;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.StreamConfiguration;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NatsTransportTest {
  private static final OpenLineage OL = new OpenLineage(URI.create("http://test.producer"));

  private static NatsTestServer server;

  private String streamName;
  private String subject;

  @BeforeAll
  static void startServer() throws Exception {
    server = NatsTestServer.start();
  }

  @AfterAll
  static void stopServer() throws Exception {
    if (server != null) {
      server.close();
    }
  }

  @BeforeEach
  void createStream() throws Exception {
    streamName = "OL_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    subject = "openlineage." + streamName.toLowerCase(Locale.ROOT);
    try (Connection nc = Nats.connect(server.getUrl())) {
      nc.jetStreamManagement()
          .addStream(
              StreamConfiguration.builder()
                  .name(streamName)
                  .subjects(subject)
                  .duplicateWindow(Duration.ofMinutes(1))
                  .allowMessageTtl()
                  .build());
    }
  }

  @AfterEach
  void deleteStream() throws Exception {
    try (Connection nc = Nats.connect(server.getUrl())) {
      nc.jetStreamManagement().deleteStream(streamName);
    }
  }

  @Test
  void jetStreamEmitPersistsEventWithMessageId() throws Exception {
    OpenLineage.RunEvent event = runEvent();

    try (NatsTransport transport = new NatsTransport(config(subject))) {
      new OpenLineageClient(transport).emit(event);
    }

    List<MessageInfo> messages = streamMessages();
    assertThat(messages).hasSize(1);
    assertThat(new String(messages.get(0).getData(), StandardCharsets.UTF_8))
        .isEqualTo(OpenLineageClientUtils.toJson(event));
    assertThat(messages.get(0).getHeaders().getFirst("Nats-Msg-Id"))
        .isEqualTo(
            event.getRun().getRunId()
                + ":START:"
                + NatsTransport.digest(
                    OpenLineageClientUtils.toJson(event).getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void jetStreamDropsDuplicatePublishes() throws Exception {
    OpenLineage.RunEvent event = runEvent();

    try (NatsTransport transport = new NatsTransport(config(subject))) {
      transport.emit(event);
      transport.emit(event);
    }

    assertThat(streamMessages()).hasSize(1);
  }

  @Test
  void jetStreamKeepsEachEventTypeOfARun() throws Exception {
    // Same run and eventTime: only eventType tells START, RUNNING, COMPLETE and FAIL apart
    UUID runId = UUID.randomUUID();
    ZonedDateTime eventTime = ZonedDateTime.now();
    List<OpenLineage.RunEvent.EventType> eventTypes =
        Arrays.asList(
            OpenLineage.RunEvent.EventType.START,
            OpenLineage.RunEvent.EventType.RUNNING,
            OpenLineage.RunEvent.EventType.COMPLETE,
            OpenLineage.RunEvent.EventType.FAIL);

    try (NatsTransport transport = new NatsTransport(config(subject))) {
      for (OpenLineage.RunEvent.EventType eventType : eventTypes) {
        transport.emit(runEvent(runId, eventType, eventTime));
      }
    }

    assertThat(streamMessages())
        .extracting(
            message ->
                OpenLineageClientUtils.runEventFromJson(
                        new String(message.getData(), StandardCharsets.UTF_8))
                    .getEventType())
        .containsExactlyElementsOf(eventTypes);
  }

  @Test
  void jetStreamKeepsDuplicatesWithoutMessageIdHeader() throws Exception {
    NatsConfig config = config(subject);
    config.setMsgIdHeader(false);
    OpenLineage.RunEvent event = runEvent();

    try (NatsTransport transport = new NatsTransport(config)) {
      transport.emit(event);
      transport.emit(event);
    }

    assertThat(streamMessages()).hasSize(2);
  }

  @Test
  void jetStreamEmitSetsMessageTtl() throws Exception {
    NatsConfig config = config(subject);
    config.setMessageTtl(3600);

    try (NatsTransport transport = new NatsTransport(config)) {
      transport.emit(runEvent());
    }

    assertThat(streamMessages().get(0).getHeaders().getFirst("Nats-TTL")).isIn("3600", "3600s");
  }

  @Test
  void jetStreamEmitThrowsWhenNoStreamCapturesSubject() throws Exception {
    NatsConfig config = config("openlineage.unbound");
    config.setPublishTimeout(1.0);

    try (NatsTransport transport = new NatsTransport(config)) {
      OpenLineage.RunEvent event = runEvent();
      assertThatThrownBy(() -> transport.emit(event))
          .isInstanceOf(OpenLineageClientException.class);
    }
  }

  @Test
  void emitsJobAndDatasetEvents() throws Exception {
    OpenLineage.JobEvent jobEvent =
        OL.newJobEventBuilder()
            .eventTime(ZonedDateTime.now())
            .job(OL.newJobBuilder().namespace("ns").name("job").build())
            .build();
    OpenLineage.DatasetEvent datasetEvent =
        OL.newDatasetEventBuilder()
            .eventTime(ZonedDateTime.now())
            .dataset(OL.newStaticDatasetBuilder().namespace("ns").name("table").build())
            .build();

    try (NatsTransport transport = new NatsTransport(config(subject))) {
      transport.emit(jobEvent);
      transport.emit(datasetEvent);
    }

    List<MessageInfo> messages = streamMessages();
    assertThat(messages).hasSize(2);
    assertThat(messages.get(0).getHeaders().getFirst("Nats-Msg-Id")).matches("job:[0-9a-f]{32}");
    assertThat(messages.get(1).getHeaders().getFirst("Nats-Msg-Id"))
        .matches("dataset:[0-9a-f]{32}");
  }

  @Test
  void emitAfterCloseReconnects() throws Exception {
    NatsConfig config = config(subject);
    config.setMsgIdHeader(false);
    NatsTransport transport = new NatsTransport(config);

    transport.emit(runEvent());
    transport.close();
    transport.emit(runEvent());
    transport.close();

    assertThat(streamMessages()).hasSize(2);
  }

  @Test
  void coreNatsEmitReachesSubscriber() throws Exception {
    String coreSubject = "openlineage.core." + UUID.randomUUID();
    NatsConfig config = config(coreSubject);
    config.setJetstream(false);
    OpenLineage.RunEvent event = runEvent();

    try (Connection nc = Nats.connect(server.getUrl());
        NatsTransport transport = new NatsTransport(config)) {
      Subscription subscription = nc.subscribe(coreSubject);
      nc.flush(Duration.ofSeconds(5));

      transport.emit(event);
      Message message = subscription.nextMessage(Duration.ofSeconds(5));

      assertThat(new String(message.getData(), StandardCharsets.UTF_8))
          .isEqualTo(OpenLineageClientUtils.toJson(event));
    }
  }

  @Test
  void authenticatesWithUserAndPassword() throws Exception {
    try (NatsTestServer authServer =
        NatsTestServer.local()
            .withoutJetStream()
            .args("--user", "ol", "--pass", "secret")
            .start()) {
      NatsConfig config = config("openlineage.auth");
      config.setUrl(authServer.getUrl());
      config.setJetstream(false);
      config.setUser("ol");
      config.setPassword("secret");

      try (NatsTransport transport = new NatsTransport(config)) {
        transport.emit(runEvent());
      }

      config.setPassword("wrong");
      try (NatsTransport transport = new NatsTransport(config)) {
        OpenLineage.RunEvent event = runEvent();
        assertThatThrownBy(() -> transport.emit(event))
            .isInstanceOf(OpenLineageClientException.class);
      }
    }
  }

  private static NatsConfig config(String subject) {
    NatsConfig config = new NatsConfig();
    config.setUrl(server.getUrl());
    config.setSubject(subject);
    return config;
  }

  private static OpenLineage.RunEvent runEvent() {
    return runEvent(UUID.randomUUID(), OpenLineage.RunEvent.EventType.START, ZonedDateTime.now());
  }

  private static OpenLineage.RunEvent runEvent(
      UUID runId, OpenLineage.RunEvent.EventType eventType, ZonedDateTime eventTime) {
    return OL.newRunEventBuilder()
        .eventType(eventType)
        .eventTime(eventTime)
        .run(OL.newRunBuilder().runId(runId).build())
        .job(OL.newJobBuilder().namespace("nats").name("test").build())
        .build();
  }

  private List<MessageInfo> streamMessages() throws Exception {
    try (Connection nc = Nats.connect(server.getUrl())) {
      JetStreamManagement jsm = nc.jetStreamManagement();
      long lastSeq = jsm.getStreamInfo(streamName).getStreamState().getLastSequence();
      List<MessageInfo> messages = new ArrayList<>();
      for (long seq = 1; seq <= lastSeq; seq++) {
        messages.add(jsm.getMessage(streamName, seq));
      }
      return messages;
    }
  }
}
