/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.fasterxml.jackson.core.type.TypeReference;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs against the published shadow jar instead of the module's classes, so relocated jnats and
 * BouncyCastle are exercised: ServiceLoader discovery and creds (NKey/Ed25519) authentication.
 */
class ShadowJarSmokeTest {
  @TempDir Path tmp;

  @Test
  void shadowJarPublishesWithCredsAuthentication() throws Exception {
    NatsSecurityFixtures.Operator operator = NatsSecurityFixtures.operator();
    Path creds = tmp.resolve("user.creds");
    Files.write(creds, operator.creds.getBytes(StandardCharsets.UTF_8));

    try (NatsTestServer server =
        NatsTestServer.local().config(operator.serverConfig).withoutJetStream().start()) {
      String yaml =
          "transport:\n"
              + "  type: nats\n"
              + "  url: "
              + server.getUrl()
              + "\n"
              + "  subject: ol.shadow\n"
              + "  jetstream: false\n"
              + "  credsFile: "
              + creds
              + "\n";
      OpenLineageConfig<?> config =
          OpenLineageClientUtils.loadOpenLineageConfigYaml(
              new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)),
              new TypeReference<OpenLineageConfig<?>>() {});

      try (Transport transport = new TransportFactory(config.getTransportConfig()).build()) {
        assertThat(
                transport.getClass().getProtectionDomain().getCodeSource().getLocation().getPath())
            .contains("transports-nats")
            .endsWith(".jar");
        assertThat(
                Class.forName(
                    "io.openlineage.client.transports.nats.shaded.io.nats.client.Nats",
                    false,
                    transport.getClass().getClassLoader()))
            .isNotNull();
        transport.emit(NatsServerScenariosTest.runEvent("shadow-jar"));
      }
    }
  }

  @Test
  void shadowJarHonoursDocumentedJnatsPropertyKeys() throws Exception {
    // Shading rewrites jnats' own property-name constants, so documented io.nats.client.* keys
    // must still reach it
    try (NatsTestServer server =
        NatsTestServer.local().withoutJetStream().args("--auth", "s3cret-token").start()) {
      String yaml =
          "transport:\n"
              + "  type: nats\n"
              + "  url: "
              + server.getUrl()
              + "\n"
              + "  subject: ol.shadow\n"
              + "  jetstream: false\n"
              + "  connectTimeout: 1\n"
              + "  properties:\n"
              + "    io.nats.client.token: s3cret-token\n";
      OpenLineageConfig<?> config =
          OpenLineageClientUtils.loadOpenLineageConfigYaml(
              new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)),
              new TypeReference<OpenLineageConfig<?>>() {});

      try (Transport transport = new TransportFactory(config.getTransportConfig()).build()) {
        assertThatCode(() -> transport.emit(NatsServerScenariosTest.runEvent("shadow-properties")))
            .doesNotThrowAnyException();
      }
    }
  }
}
