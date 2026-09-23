/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.NKey;
import io.nats.client.Nats;
import io.nats.client.api.StreamConfiguration;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.transports.CompositeConfig;
import io.openlineage.client.transports.CompositeTransport;
import io.openlineage.client.transports.FileConfig;
import java.io.File;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Behaviour against real servers with auth, TLS, outages and unusual events. */
class NatsServerScenariosTest {
  private static final OpenLineage OL = new OpenLineage(URI.create("http://test.producer"));
  private static final double FAST_SECONDS = 1.0;

  @TempDir Path tmp;

  @Test
  void authenticatesWithCredsFile() throws Exception {
    NatsSecurityFixtures.Operator trusted = NatsSecurityFixtures.operator();
    NatsSecurityFixtures.Operator untrusted = NatsSecurityFixtures.operator();
    Path trustedCreds = write("trusted.creds", trusted.creds);
    Path untrustedCreds = write("untrusted.creds", untrusted.creds);

    try (NatsTestServer server =
        NatsTestServer.local().config(trusted.serverConfig).withoutJetStream().start()) {
      NatsConfig allowed = coreConfig(server.getUrl());
      allowed.setCredsFile(trustedCreds.toString());
      try (NatsTransport transport = new NatsTransport(allowed)) {
        transport.emit(runEvent("job"));
      }

      NatsConfig denied = coreConfig(server.getUrl());
      denied.setCredsFile(untrustedCreds.toString());
      try (NatsTransport transport = new NatsTransport(denied)) {
        OpenLineage.RunEvent event = runEvent("job");
        assertThatThrownBy(() -> transport.emit(event))
            .isInstanceOf(OpenLineageClientException.class);
      }
    }
  }

  @Test
  void tlsTrustsServerSignedByConfiguredTruststore() throws Exception {
    NatsSecurityFixtures.Certificates certs = NatsSecurityFixtures.certificates(tmp);

    try (NatsTestServer server = tlsServer(certs.serverCert, certs.serverKey)) {
      NatsConfig config = coreConfig(server.getUrl());
      trust(config, certs);
      try (NatsTransport transport = new NatsTransport(config)) {
        assertThatCode(() -> transport.emit(runEvent("job"))).doesNotThrowAnyException();
      }
    }
  }

  @Test
  void tlsRejectsServerSignedByUnknownCa() throws Exception {
    NatsSecurityFixtures.Certificates certs = NatsSecurityFixtures.certificates(tmp);

    try (NatsTestServer server = tlsServer(certs.serverCert, certs.serverKey)) {
      NatsConfig config = coreConfig(server.getUrl().replace("nats://", "tls://"));
      try (NatsTransport transport = new NatsTransport(config)) {
        OpenLineage.RunEvent event = runEvent("job");
        assertThatThrownBy(() -> transport.emit(event))
            .isInstanceOf(OpenLineageClientException.class);
      }
    }
  }

  @Test
  void tlsAcceptsACertificateWithOnlyADnsNameWhenConnectingByName() throws Exception {
    // What an operator's certificate actually looks like: a DNS SAN and no IP SAN, reached by
    // name. jnats rewrites the URL host to the resolved IP before connecting, so verification
    // has to be done against the configured host, not the socket's peer.
    NatsSecurityFixtures.Certificates certs = NatsSecurityFixtures.certificates(tmp);

    try (NatsTestServer server = tlsServer(certs.dnsOnlyCert, certs.dnsOnlyKey)) {
      String byName =
          server.getUrl().replace("nats://", "tls://").replace("127.0.0.1", "localhost");
      NatsConfig config = coreConfig(byName);
      trust(config, certs);
      try (NatsTransport transport = new NatsTransport(config)) {
        assertThatCode(() -> transport.emit(runEvent("job"))).doesNotThrowAnyException();
      }
    }
  }

  @Test
  void tlsRejectsCertificateIssuedForAnotherHost() throws Exception {
    NatsSecurityFixtures.Certificates certs = NatsSecurityFixtures.certificates(tmp);

    try (NatsTestServer server = tlsServer(certs.wrongHostCert, certs.wrongHostKey)) {
      NatsConfig config = coreConfig(server.getUrl());
      trust(config, certs);
      try (NatsTransport transport = new NatsTransport(config)) {
        OpenLineage.RunEvent event = runEvent("job");
        assertThatThrownBy(() -> transport.emit(event))
            .isInstanceOf(OpenLineageClientException.class);
      }
    }
  }

  @Test
  void mutualTlsSendsClientCertificate() throws Exception {
    NatsSecurityFixtures.Certificates certs = NatsSecurityFixtures.certificates(tmp);

    try (NatsTestServer server =
        NatsTestServer.local()
            .withoutJetStream()
            .args(
                "--tlsverify",
                "--tlscert",
                certs.serverCert.toString(),
                "--tlskey",
                certs.serverKey.toString(),
                "--tlscacert",
                certs.ca.toString())
            .start()) {
      NatsConfig withCert = coreConfig(server.getUrl());
      trust(withCert, certs);
      withCert.setTlsKeystorePath(certs.clientKeystore.toString());
      withCert.setTlsKeystorePassword(NatsSecurityFixtures.STORE_PASSWORD);
      try (NatsTransport transport = new NatsTransport(withCert)) {
        transport.emit(runEvent("job"));
      }

      NatsConfig withoutCert = coreConfig(server.getUrl());
      trust(withoutCert, certs);
      try (NatsTransport transport = new NatsTransport(withoutCert)) {
        OpenLineage.RunEvent event = runEvent("job");
        assertThatThrownBy(() -> transport.emit(event))
            .isInstanceOf(OpenLineageClientException.class);
      }
    }
  }

  @Test
  void emitFailsWithinTimeoutsWhenServerIsDown() throws Exception {
    NatsConfig config = jetStreamConfig("nats://127.0.0.1:" + NatsTestServer.freePort(), "ol.down");

    try (NatsTransport transport = new NatsTransport(config)) {
      OpenLineage.RunEvent event = runEvent("job");
      long started = System.nanoTime();
      assertThatThrownBy(() -> transport.emit(event))
          .isInstanceOf(OpenLineageClientException.class);
      assertThat(Duration.ofNanos(System.nanoTime() - started))
          .isLessThan(Duration.ofSeconds((long) (2 * FAST_SECONDS) + 2));
    }
  }

  @Test
  void emitSucceedsAfterServerRestart() throws Exception {
    int port = NatsTestServer.freePort();
    Path store = tmp.resolve("store");
    NatsConfig config = jetStreamConfig("nats://127.0.0.1:" + port, "ol.restart");
    config.setMsgIdHeader(false);
    config.setPublishTimeout(5.0);

    try (NatsTransport transport = new NatsTransport(config)) {
      try (NatsTestServer server = NatsTestServer.local().port(port).store(store).start()) {
        addStream(server.getUrl(), "RESTART", "ol.restart");
        transport.emit(runEvent("job"));
      }
      try (NatsTestServer server = NatsTestServer.local().port(port).store(store).start()) {
        transport.emit(runEvent("job"));
        assertThat(streamMessageCount(server.getUrl(), "RESTART")).isEqualTo(2);
      }
    }
  }

  @Test
  void compositeTransportContinuesWhenNatsFails() throws Exception {
    try (NatsTestServer server = NatsTestServer.start()) {
      NatsConfig nats = jetStreamConfig(server.getUrl(), "openlineage.unbound");
      FileConfig file = new FileConfig();
      File output = tmp.resolve("events.jsonl").toFile();
      file.setLocation(output.getAbsolutePath());
      CompositeConfig composite =
          CompositeConfig.createFromTransportConfigs(Arrays.asList(nats, file), true, true);

      try (CompositeTransport transport = new CompositeTransport(composite)) {
        transport.emit(runEvent("composite-job"));
      }

      assertThat(new String(Files.readAllBytes(output.toPath()), StandardCharsets.UTF_8))
          .contains("composite-job");
    }
  }

  @Test
  void publishesEventsWithNonAsciiAndMultiLineNames() throws Exception {
    try (NatsTestServer server = NatsTestServer.start()) {
      String subject = "ol.names." + UUID.randomUUID();
      String stream = "NAMES_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
      addStream(server.getUrl(), stream, subject);

      try (NatsTransport transport = new NatsTransport(jetStreamConfig(server.getUrl(), subject))) {
        transport.emit(jobEvent("ns", "ränta_öresavrundning"));
        transport.emit(jobEvent("ns", "job\r\nX-Injected: yes"));
      }

      assertThat(streamMessageCount(server.getUrl(), stream)).isEqualTo(2);
    }
  }

  @Test
  void oversizedEventFailsWithOpenLineageClientException() throws Exception {
    try (NatsTestServer server = NatsTestServer.local().config("max_payload: 1024\n").start()) {
      NatsConfig config = jetStreamConfig(server.getUrl(), "ol.big");
      config.setJetstream(false);
      char[] longName = new char[4096];
      Arrays.fill(longName, 'x');

      try (NatsTransport transport = new NatsTransport(config)) {
        OpenLineage.RunEvent event = runEvent(new String(longName));
        assertThatThrownBy(() -> transport.emit(event))
            .isInstanceOf(OpenLineageClientException.class);
      }
    }
  }

  @Test
  void userWithoutPasswordIsRejectedWithAClearMessage() {
    NatsConfig config = coreConfig("nats://127.0.0.1:4222");
    config.setUser("ol");

    assertThatThrownBy(() -> new NatsTransport(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("password");
  }

  @Test
  void jvmExitsAfterEmitWithoutClose() throws Exception {
    try (NatsTestServer server = NatsTestServer.start()) {
      String java = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
      Process process =
          new ProcessBuilder(
                  java,
                  "-cp",
                  System.getProperty("java.class.path"),
                  EmitOnceWithoutClose.class.getName(),
                  server.getUrl())
              .redirectErrorStream(true)
              .redirectOutput(tmp.resolve("emit-once.log").toFile())
              .start();

      boolean exited = process.waitFor(30, TimeUnit.SECONDS);
      if (!exited) {
        process.destroyForcibly();
      }

      assertThat(exited).as("JVM still running 30s after main returned").isTrue();
      assertThat(process.exitValue()).isZero();
    }
  }

  @Test
  void authenticatesWithNkeySeed() throws Exception {
    NKey user = NKey.createUser(new SecureRandom());
    NKey intruder = NKey.createUser(new SecureRandom());
    Path userSeed = write("user.nk", new String(user.getSeed()));
    Path intruderSeed = write("intruder.nk", new String(intruder.getSeed()));
    String serverConfig =
        "authorization { users = [ { nkey: " + new String(user.getPublicKey()) + " } ] }\n";

    try (NatsTestServer server =
        NatsTestServer.local().config(serverConfig).withoutJetStream().start()) {
      NatsConfig allowed = coreConfig(server.getUrl());
      allowed.setNkeysSeed(userSeed.toString());
      try (NatsTransport transport = new NatsTransport(allowed)) {
        transport.emit(runEvent("job"));
      }

      NatsConfig denied = coreConfig(server.getUrl());
      denied.setNkeysSeed(intruderSeed.toString());
      try (NatsTransport transport = new NatsTransport(denied)) {
        OpenLineage.RunEvent event = runEvent("job");
        assertThatThrownBy(() -> transport.emit(event))
            .isInstanceOf(OpenLineageClientException.class);
      }
    }
  }

  @Test
  void failsFastAfterAConnectionAttemptTimedOut() throws Exception {
    // Accepts TCP connections but never sends the NATS INFO greeting, like an unreachable host
    try (ServerSocket blackHole = new ServerSocket(0)) {
      NatsConfig config = coreConfig("nats://127.0.0.1:" + blackHole.getLocalPort());
      config.setConnectTimeout(2.0);

      try (NatsTransport transport = new NatsTransport(config)) {
        OpenLineage.RunEvent event = runEvent("job");
        assertThatThrownBy(() -> transport.emit(event))
            .isInstanceOf(OpenLineageClientException.class);

        long started = System.nanoTime();
        assertThatThrownBy(() -> transport.emit(event))
            .isInstanceOf(OpenLineageClientException.class)
            .hasMessageContaining("NATS unavailable");
        assertThat(Duration.ofNanos(System.nanoTime() - started))
            .isLessThan(Duration.ofMillis(500));
      }
    }
  }

  private NatsTestServer tlsServer(Path cert, Path key) throws Exception {
    return NatsTestServer.local()
        .withoutJetStream()
        .args("--tls", "--tlscert", cert.toString(), "--tlskey", key.toString())
        .start();
  }

  private static void trust(NatsConfig config, NatsSecurityFixtures.Certificates certs) {
    config.setTlsTruststorePath(certs.truststore.toString());
    config.setTlsTruststorePassword(NatsSecurityFixtures.STORE_PASSWORD);
  }

  private Path write(String name, String content) throws Exception {
    Path file = tmp.resolve(name);
    Files.write(file, content.getBytes(StandardCharsets.UTF_8));
    return file;
  }

  private static NatsConfig coreConfig(String url) {
    NatsConfig config = jetStreamConfig(url, "ol.scenarios");
    config.setJetstream(false);
    return config;
  }

  private static NatsConfig jetStreamConfig(String url, String subject) {
    NatsConfig config = new NatsConfig();
    config.setUrl(url);
    config.setSubject(subject);
    config.setConnectTimeout(FAST_SECONDS);
    config.setPublishTimeout(FAST_SECONDS);
    return config;
  }

  private static void addStream(String url, String name, String subject) throws Exception {
    try (Connection nc = Nats.connect(url)) {
      nc.jetStreamManagement()
          .addStream(StreamConfiguration.builder().name(name).subjects(subject).build());
    }
  }

  private static long streamMessageCount(String url, String stream) throws Exception {
    try (Connection nc = Nats.connect(url)) {
      JetStreamManagement jsm = nc.jetStreamManagement();
      return jsm.getStreamInfo(stream).getStreamState().getMsgCount();
    }
  }

  static OpenLineage.RunEvent runEvent(String jobName) {
    return OL.newRunEventBuilder()
        .eventType(OpenLineage.RunEvent.EventType.START)
        .eventTime(ZonedDateTime.now())
        .run(OL.newRunBuilder().runId(UUID.randomUUID()).build())
        .job(OL.newJobBuilder().namespace("nats").name(jobName).build())
        .build();
  }

  private static OpenLineage.JobEvent jobEvent(String namespace, String name) {
    return OL.newJobEventBuilder()
        .eventTime(ZonedDateTime.now())
        .job(OL.newJobBuilder().namespace(namespace).name(name).build())
        .build();
  }
}
