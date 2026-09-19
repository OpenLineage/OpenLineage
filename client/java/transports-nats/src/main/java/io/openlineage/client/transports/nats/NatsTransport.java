/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.nats;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.Transport;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Publishes OpenLineage events to a NATS subject. By default events go through JetStream and each
 * emit waits for the stream's acknowledgement; with {@code jetstream: false} they are published
 * over core NATS.
 */
@Slf4j
public class NatsTransport extends Transport {
  private static final double DEFAULT_PUBLISH_TIMEOUT_SECONDS = 5.0;
  private static final double DEFAULT_CONNECT_TIMEOUT_SECONDS = 5.0;
  private static final int MAX_AUTH_METHODS = 1;

  private final String subject;
  private final boolean jetstream;
  private final boolean msgIdHeader;
  private final Integer messageTtl;
  private final Duration publishTimeout;
  private final Options options;

  private Connection connection;
  private JetStream jetStreamContext;

  public NatsTransport(@NonNull final NatsConfig config) {
    validate(config);
    this.subject = config.getSubject();
    this.jetstream = !Boolean.FALSE.equals(config.getJetstream());
    this.msgIdHeader = !Boolean.FALSE.equals(config.getMsgIdHeader());
    this.messageTtl = config.getMessageTtl();
    this.publishTimeout = seconds(config.getPublishTimeout(), DEFAULT_PUBLISH_TIMEOUT_SECONDS);
    this.options = buildOptions(config);
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    emit(runEvent, publishTimeout);
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent, @NonNull Duration timeout) {
    publish(OpenLineageClientUtils.toJson(runEvent), messageId(runEvent), timeout);
  }

  @Override
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    publish(OpenLineageClientUtils.toJson(datasetEvent), messageId(datasetEvent), publishTimeout);
  }

  @Override
  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
    publish(OpenLineageClientUtils.toJson(jobEvent), messageId(jobEvent), publishTimeout);
  }

  @Override
  public synchronized void close() throws Exception {
    if (connection != null) {
      connection.close();
    }
  }

  static String messageId(OpenLineage.RunEvent event) {
    return "run:"
        + event.getRun().getRunId()
        + ":"
        + event.getEventType()
        + ":"
        + isoTime(event.getEventTime());
  }

  static String messageId(OpenLineage.JobEvent event) {
    return "job:"
        + event.getJob().getNamespace()
        + "/"
        + event.getJob().getName()
        + ":"
        + isoTime(event.getEventTime());
  }

  static String messageId(OpenLineage.DatasetEvent event) {
    return "dataset:"
        + event.getDataset().getNamespace()
        + "/"
        + event.getDataset().getName()
        + ":"
        + isoTime(event.getEventTime());
  }

  private void publish(String eventAsJson, String messageId, Duration timeout) {
    byte[] payload = eventAsJson.getBytes(StandardCharsets.UTF_8);
    try {
      if (jetstream) {
        PublishOptions.Builder publishOptions = PublishOptions.builder().streamTimeout(timeout);
        if (msgIdHeader) {
          publishOptions.messageId(messageId);
        }
        if (messageTtl != null) {
          publishOptions.messageTtlSeconds(messageTtl);
        }
        PublishAck ack = jetStream().publish(subject, payload, publishOptions.build());
        log.debug(
            "Published lineage event to stream {} with sequence {}, duplicate={}",
            ack.getStream(),
            ack.getSeqno(),
            ack.isDuplicate());
      } else {
        Connection nc = connection();
        nc.publish(subject, payload);
        nc.flush(timeout);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new OpenLineageClientException("Interrupted while publishing lineage event to NATS", e);
    } catch (IOException | JetStreamApiException | TimeoutException e) {
      throw new OpenLineageClientException(
          "Failed to publish lineage event to NATS subject " + subject, e);
    }
  }

  private synchronized Connection connection() throws IOException, InterruptedException {
    if (connection == null || connection.getStatus() == Connection.Status.CLOSED) {
      connection = Nats.connect(options);
      if (jetstream) {
        jetStreamContext = connection.jetStream();
      }
    }
    return connection;
  }

  private synchronized JetStream jetStream() throws IOException, InterruptedException {
    connection();
    return jetStreamContext;
  }

  private static void validate(NatsConfig config) {
    if (isBlank(config.getUrl())) {
      throw new IllegalArgumentException("NATS transport requires `url`");
    }
    if (isBlank(config.getSubject())) {
      throw new IllegalArgumentException("NATS transport requires `subject`");
    }
    List<String> authMethods = new ArrayList<>();
    if (config.getUser() != null || config.getPassword() != null) {
      authMethods.add("user/password");
    }
    if (config.getToken() != null) {
      authMethods.add("token");
    }
    if (config.getCredsFile() != null) {
      authMethods.add("credsFile");
    }
    if (authMethods.size() > MAX_AUTH_METHODS) {
      throw new IllegalArgumentException(
          "NATS transport accepts one authentication method, got: "
              + String.join(", ", authMethods));
    }
    if (config.getMessageTtl() != null && Boolean.FALSE.equals(config.getJetstream())) {
      throw new IllegalArgumentException("NATS transport `messageTtl` requires `jetstream: true`");
    }
  }

  private static Options buildOptions(NatsConfig config) {
    Options.Builder builder = new Options.Builder();
    if (config.getProperties() != null && !config.getProperties().isEmpty()) {
      builder.properties(config.getProperties());
    }
    List<String> servers =
        Arrays.stream(config.getUrl().split(","))
            .map(String::trim)
            .filter(server -> !server.isEmpty())
            .collect(Collectors.toList());
    builder
        .servers(servers.toArray(new String[0]))
        .connectionName("openlineage-java")
        .connectionTimeout(seconds(config.getConnectTimeout(), DEFAULT_CONNECT_TIMEOUT_SECONDS));
    if (config.getUser() != null || config.getPassword() != null) {
      builder.userInfo(config.getUser(), config.getPassword());
    }
    if (config.getToken() != null) {
      builder.token(config.getToken().toCharArray());
    }
    if (config.getCredsFile() != null) {
      builder.credentialPath(config.getCredsFile());
    }
    if (config.getTlsKeystorePath() != null) {
      builder.keystorePath(config.getTlsKeystorePath());
      if (config.getTlsKeystorePassword() != null) {
        builder.keystorePassword(config.getTlsKeystorePassword().toCharArray());
      }
    }
    if (config.getTlsTruststorePath() != null) {
      builder.truststorePath(config.getTlsTruststorePath());
      if (config.getTlsTruststorePassword() != null) {
        builder.truststorePassword(config.getTlsTruststorePassword().toCharArray());
      }
    }
    return builder.build();
  }

  private static Duration seconds(Double value, double defaultValue) {
    double secondsValue = value == null ? defaultValue : value;
    return Duration.ofMillis(Math.round(secondsValue * 1000));
  }

  private static String isoTime(ZonedDateTime time) {
    return time == null ? null : DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(time);
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
