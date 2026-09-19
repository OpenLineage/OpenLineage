/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.nats;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.support.SSLUtils;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.Transport;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
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
  private static final double MIN_TIMEOUT_SECONDS = 0.001;
  private static final int MAX_AUTH_METHODS = 1;
  private static final int MIN_MESSAGE_TTL_SECONDS = 1;
  private static final int MESSAGE_ID_DIGEST_CHARS = 32;

  // Built at runtime: the shadow jar rewrites string constants that start with jnats' package,
  // including jnats' own property names, and documented keys must be mapped onto those
  private static final String DOCUMENTED_PROPERTY_PREFIX =
      String.join(".", "io", "nats", "client") + ".";
  private static final String JNATS_PROPERTY_PREFIX =
      Options.PROP_URL.substring(0, Options.PROP_URL.lastIndexOf('.') + 1);

  private final NatsConfig config;
  private final String subject;
  private final boolean jetstream;
  private final boolean msgIdHeader;
  private final Integer messageTtl;
  private final Duration publishTimeout;
  private final Duration connectTimeout;

  private Connection connection;
  private JetStream jetStreamContext;
  private ExecutorService executor;
  private ScheduledExecutorService scheduledExecutor;
  private long lastConnectFailureNanos;
  private Optional<Exception> lastConnectFailure = Optional.empty();

  public NatsTransport(@NonNull final NatsConfig config) {
    validate(config);
    this.config = config;
    this.subject = config.getSubject();
    this.jetstream = !Boolean.FALSE.equals(config.getJetstream());
    this.msgIdHeader = !Boolean.FALSE.equals(config.getMsgIdHeader());
    this.messageTtl = config.getMessageTtl();
    this.publishTimeout = seconds(config.getPublishTimeout(), DEFAULT_PUBLISH_TIMEOUT_SECONDS);
    this.connectTimeout = seconds(config.getConnectTimeout(), DEFAULT_CONNECT_TIMEOUT_SECONDS);
    // Fail on unusable TLS or credential settings now rather than on the first emit
    buildOptions(daemonThreads());
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    emit(runEvent, publishTimeout);
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent, @NonNull Duration timeout) {
    byte[] payload = OpenLineageClientUtils.toJson(runEvent).getBytes(StandardCharsets.UTF_8);
    String id =
        runEvent.getRun().getRunId() + ":" + runEvent.getEventType() + ":" + digest(payload);
    publish(payload, id, timeout);
  }

  @Override
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    byte[] payload = OpenLineageClientUtils.toJson(datasetEvent).getBytes(StandardCharsets.UTF_8);
    publish(payload, "dataset:" + digest(payload), publishTimeout);
  }

  @Override
  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
    byte[] payload = OpenLineageClientUtils.toJson(jobEvent).getBytes(StandardCharsets.UTF_8);
    publish(payload, "job:" + digest(payload), publishTimeout);
  }

  @Override
  public synchronized void close() throws Exception {
    try {
      if (connection != null) {
        connection.close();
      }
    } finally {
      shutdownExecutors();
    }
  }

  /**
   * Digest of the serialized event: a retried publish of the same event repeats the id, any two
   * different events get different ids, and the id never contains characters that NATS headers
   * reject.
   */
  static String digest(byte[] payload) {
    try {
      byte[] hash = MessageDigest.getInstance("SHA-256").digest(payload);
      StringBuilder hex = new StringBuilder(hash.length * 2);
      for (byte b : hash) {
        hex.append(String.format("%02x", b));
      }
      return hex.substring(0, MESSAGE_ID_DIGEST_CHARS);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required by every Java platform", e);
    }
  }

  private void publish(byte[] payload, String messageId, Duration timeout) {
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
    } catch (OpenLineageClientException e) {
      throw e;
    } catch (Exception e) {
      // jnats also reports oversized payloads, closed connections and bad arguments with
      // unchecked exceptions; callers such as the Spark listener only expect this one type
      throw new OpenLineageClientException(
          "Failed to publish lineage event to NATS subject " + subject, e);
    }
  }

  private synchronized Connection connection() throws Exception {
    if (connection != null && connection.getStatus() != Connection.Status.CLOSED) {
      return connection;
    }
    long sinceFailure = System.nanoTime() - lastConnectFailureNanos;
    if (lastConnectFailure.isPresent() && sinceFailure < connectTimeout.toNanos()) {
      // Every connect attempt can block for connectTimeout; do not pay that on each event
      throw new OpenLineageClientException(
          "NATS unavailable: the last connection attempt failed "
              + Duration.ofNanos(sinceFailure).toMillis()
              + " ms ago",
          lastConnectFailure.get());
    }
    shutdownExecutors();
    ThreadFactory threads = daemonThreads();
    executor = Executors.newCachedThreadPool(threads);
    scheduledExecutor = Executors.newSingleThreadScheduledExecutor(threads);
    try {
      connection = Nats.connect(buildOptions(threads));
      if (jetstream) {
        jetStreamContext = connection.jetStream();
      }
      lastConnectFailure = Optional.empty();
      return connection;
    } catch (Exception e) {
      lastConnectFailure = Optional.of(e);
      lastConnectFailureNanos = System.nanoTime();
      throw e;
    }
  }

  private synchronized JetStream jetStream() throws Exception {
    connection();
    return jetStreamContext;
  }

  private static ThreadFactory daemonThreads() {
    AtomicInteger counter = new AtomicInteger();
    return runnable -> {
      // jnats threads are non-daemon by default and would keep the JVM alive after main returns
      // when the transport is not closed
      Thread thread = new Thread(runnable, "openlineage-nats-" + counter.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }

  private void shutdownExecutors() {
    if (executor != null) {
      executor.shutdownNow();
    }
    if (scheduledExecutor != null) {
      scheduledExecutor.shutdownNow();
    }
  }

  private Options buildOptions(ThreadFactory threads) {
    Options.Builder builder = new Options.Builder();
    Properties properties = jnatsProperties(config.getProperties());
    if (!properties.isEmpty()) {
      builder.properties(properties);
    }
    List<String> servers =
        Arrays.stream(config.getUrl().split(","))
            .map(String::trim)
            .filter(server -> !server.isEmpty())
            .collect(Collectors.toList());
    builder
        .servers(servers.toArray(new String[0]))
        .executor(executor)
        .scheduledExecutor(scheduledExecutor)
        .connectThreadFactory(threads)
        .callbackThreadFactory(threads)
        .readerThreadFactory(threads)
        .writerThreadFactory(threads);
    if (!properties.containsKey(Options.PROP_CONNECTION_NAME)) {
      builder.connectionName("openlineage-java");
    }
    if (config.getConnectTimeout() != null
        || !properties.containsKey(Options.PROP_CONNECTION_TIMEOUT)) {
      builder.connectionTimeout(connectTimeout);
    }
    if (config.getUser() != null) {
      builder.userInfo(config.getUser(), config.getPassword());
    }
    if (config.getToken() != null) {
      builder.token(config.getToken().toCharArray());
    }
    if (config.getNkeysSeed() != null) {
      builder.authHandler(Nats.staticCredentials(null, readSeed(config.getNkeysSeed())));
    }
    if (config.getCredsFile() != null) {
      builder.credentialPath(config.getCredsFile());
    }
    SSLContext sslContext = sslContext(servers);
    if (sslContext != null) {
      builder.sslContext(HostnameVerifyingSslContext.wrap(sslContext));
    }
    return builder.build();
  }

  private SSLContext sslContext(List<String> servers) {
    try {
      if (config.getTlsKeystorePath() != null || config.getTlsTruststorePath() != null) {
        return SSLUtils.createSSLContext(
            config.getTlsKeystorePath(),
            chars(config.getTlsKeystorePassword()),
            config.getTlsTruststorePath(),
            chars(config.getTlsTruststorePassword()));
      }
      if (servers.stream().anyMatch(server -> server.startsWith("tls://"))) {
        return SSLContext.getDefault();
      }
      return null;
    } catch (Exception e) {
      throw new IllegalArgumentException("NATS transport could not set up TLS: " + e, e);
    }
  }

  private static Properties jnatsProperties(Properties configured) {
    Properties properties = new Properties();
    if (configured != null) {
      configured.forEach(
          (key, value) -> {
            String name = String.valueOf(key);
            properties.put(
                name.startsWith(DOCUMENTED_PROPERTY_PREFIX)
                    ? JNATS_PROPERTY_PREFIX + name.substring(DOCUMENTED_PROPERTY_PREFIX.length())
                    : name,
                value);
          });
    }
    return properties;
  }

  private static char[] readSeed(String path) {
    try {
      return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
          .trim()
          .toCharArray();
    } catch (Exception e) {
      throw new IllegalArgumentException("NATS transport could not read nkeysSeed " + path, e);
    }
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
    if (config.getNkeysSeed() != null) {
      authMethods.add("nkeysSeed");
    }
    if (config.getCredsFile() != null) {
      authMethods.add("credsFile");
    }
    if (authMethods.size() > MAX_AUTH_METHODS) {
      throw new IllegalArgumentException(
          "NATS transport accepts one authentication method, got: "
              + String.join(", ", authMethods));
    }
    if ((config.getUser() == null) != (config.getPassword() == null)) {
      throw new IllegalArgumentException("NATS transport needs both `user` and `password`");
    }
    validateTimeout("publishTimeout", config.getPublishTimeout());
    validateTimeout("connectTimeout", config.getConnectTimeout());
    if (config.getMessageTtl() != null) {
      if (Boolean.FALSE.equals(config.getJetstream())) {
        throw new IllegalArgumentException(
            "NATS transport `messageTtl` requires `jetstream: true`");
      }
      if (config.getMessageTtl() < MIN_MESSAGE_TTL_SECONDS) {
        throw new IllegalArgumentException(
            "NATS transport `messageTtl` must be at least 1 second, got " + config.getMessageTtl());
      }
    }
  }

  private static void validateTimeout(String name, Double value) {
    // Below a millisecond the timeout rounds to zero, which jnats treats as "wait forever"
    if (value != null && !(value >= MIN_TIMEOUT_SECONDS && !value.isInfinite())) {
      throw new IllegalArgumentException(
          "NATS transport `"
              + name
              + "` must be a finite number of seconds >= 0.001, got "
              + value);
    }
  }

  private static Duration seconds(Double value, double defaultValue) {
    double secondsValue = value == null ? defaultValue : value;
    return Duration.ofMillis(Math.round(secondsValue * 1000));
  }

  private static char[] chars(String value) {
    return value == null ? null : value.toCharArray();
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
