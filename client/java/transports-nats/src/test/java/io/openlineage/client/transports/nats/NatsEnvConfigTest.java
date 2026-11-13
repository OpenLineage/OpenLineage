/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.nats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import io.openlineage.client.Environment;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.transports.TransportFactory;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Configuration through OPENLINEAGE__ environment variables, as Spark and Flink jobs use it. */
class NatsEnvConfigTest {

  @Test
  void environmentVariablesBuildNatsTransport() {
    Map<String, String> env = new HashMap<>();
    env.put("OPENLINEAGE__TRANSPORT__TYPE", "nats");
    env.put("OPENLINEAGE__TRANSPORT__URL", "nats://Nats-1:4222,nats://Nats-2:4222");
    env.put("OPENLINEAGE__TRANSPORT__SUBJECT", "OpenLineage.Events");
    env.put("OPENLINEAGE__TRANSPORT__PUBLISH_TIMEOUT", "2.5");
    env.put("OPENLINEAGE__TRANSPORT__MSG_ID_HEADER", "false");
    env.put("OPENLINEAGE__TRANSPORT__MESSAGE_TTL", "600");
    env.put("OPENLINEAGE__TRANSPORT__USER", "OpenLineage");
    env.put("OPENLINEAGE__TRANSPORT__PASSWORD", "MixedCase-Secret");

    NatsConfig config;
    try (MockedStatic<Environment> mocked = mockStatic(Environment.class)) {
      when(Environment.getAllEnvironmentVariables()).thenReturn(env);
      OpenLineageConfig<?> olConfig =
          OpenLineageClientUtils.loadOpenLineageConfigFromEnvVars(
              new TypeReference<OpenLineageConfig<?>>() {});
      config = (NatsConfig) olConfig.getTransportConfig();
    }

    assertThat(config.getUrl()).isEqualTo("nats://Nats-1:4222,nats://Nats-2:4222");
    assertThat(config.getSubject()).isEqualTo("OpenLineage.Events");
    assertThat(config.getPublishTimeout()).isEqualTo(2.5);
    assertThat(config.getMsgIdHeader()).isFalse();
    assertThat(config.getMessageTtl()).isEqualTo(600);
    assertThat(config.getUser()).isEqualTo("OpenLineage");
    assertThat(config.getPassword()).isEqualTo("MixedCase-Secret");
    assertThat(new TransportFactory(config).build()).isInstanceOf(NatsTransport.class);
  }
}
