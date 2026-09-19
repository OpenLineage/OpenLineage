/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.type.TypeReference;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.transports.TransportFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class NatsConfigTest {
  private final String subject = "openlineage.events";

  @Test
  void yamlConfigBuildsNatsTransport() throws Exception {
    String yaml =
        "transport:\n"
            + "  type: nats\n"
            + "  url: nats://a:4222, nats://b:4222\n"
            + "  subject: openlineage.events\n"
            + "  publishTimeout: 2.5\n"
            + "  messageTtl: 60\n"
            + "  credsFile: /etc/nats/openlineage.creds\n"
            + "  properties:\n"
            + "    io.nats.client.reconnect.max: 10\n";

    OpenLineageConfig<?> olConfig =
        OpenLineageClientUtils.loadOpenLineageConfigYaml(
            new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)),
            new TypeReference<OpenLineageConfig<?>>() {});

    assertThat(olConfig.getTransportConfig()).isInstanceOf(NatsConfig.class);
    NatsConfig config = (NatsConfig) olConfig.getTransportConfig();
    assertThat(config.getSubject()).isEqualTo("openlineage.events");
    assertThat(config.getPublishTimeout()).isEqualTo(2.5);
    assertThat(config.getMessageTtl()).isEqualTo(60);
    assertThat(config.getCredsFile()).isEqualTo("/etc/nats/openlineage.creds");
    assertThat(config.getProperties()).containsEntry("io.nats.client.reconnect.max", "10");
    assertThat(new TransportFactory(config).build()).isInstanceOf(NatsTransport.class);
  }

  @Test
  void mergeKeepsExistingValuesAndOverridesNonNull() {
    Properties properties = new Properties();
    properties.setProperty("io.nats.client.reconnect.max", "10");
    NatsConfig base = config("openlineage.base");
    base.setProperties(properties);
    NatsConfig override = new NatsConfig();
    override.setSubject("openlineage.override");
    override.setPublishTimeout(1.0);

    NatsConfig merged = base.mergeWith(override);

    assertThat(merged.getUrl()).isEqualTo(base.getUrl());
    assertThat(merged.getSubject()).isEqualTo("openlineage.override");
    assertThat(merged.getPublishTimeout()).isEqualTo(1.0);
    assertThat(merged.getProperties()).containsEntry("io.nats.client.reconnect.max", "10");
  }

  @Test
  void rejectsInvalidConfig() {
    NatsConfig missingSubject = config(null);
    NatsConfig twoAuthMethods = config(subject);
    twoAuthMethods.setToken("t");
    twoAuthMethods.setCredsFile("/c");
    NatsConfig ttlWithoutJetStream = config(subject);
    ttlWithoutJetStream.setJetstream(false);
    ttlWithoutJetStream.setMessageTtl(10);

    assertThatThrownBy(() -> new NatsTransport(missingSubject)).hasMessageContaining("subject");
    assertThatThrownBy(() -> new NatsTransport(twoAuthMethods))
        .hasMessageContaining("one authentication method");
    assertThatThrownBy(() -> new NatsTransport(ttlWithoutJetStream))
        .hasMessageContaining("jetstream");
  }

  @Test
  void rejectsTimeoutsThatCannotWork() {
    for (double timeout : new double[] {0.0, -1.0, 0.0001, Double.NaN, Double.POSITIVE_INFINITY}) {
      NatsConfig publish = config(subject);
      publish.setPublishTimeout(timeout);
      NatsConfig connect = config(subject);
      connect.setConnectTimeout(timeout);

      assertThatThrownBy(() -> new NatsTransport(publish)).hasMessageContaining("publishTimeout");
      assertThatThrownBy(() -> new NatsTransport(connect)).hasMessageContaining("connectTimeout");
    }
  }

  @Test
  void rejectsMessageTtlBelowOneSecond() {
    NatsConfig config = config(subject);
    config.setMessageTtl(0);

    assertThatThrownBy(() -> new NatsTransport(config)).hasMessageContaining("messageTtl");
  }

  @Test
  void mergeToleratesMissingProperties() {
    NatsConfig base = config(subject);
    base.setProperties(null);
    NatsConfig override = new NatsConfig();
    override.setProperties(null);

    assertThat(base.mergeWith(override).getProperties()).isEmpty();
  }

  private static NatsConfig config(String subject) {
    NatsConfig config = new NatsConfig();
    config.setUrl("nats://localhost:4222");
    config.setSubject(subject);
    return config;
  }
}
