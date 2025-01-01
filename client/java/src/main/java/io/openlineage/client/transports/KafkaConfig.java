/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.MergeConfig;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
@AllArgsConstructor
public final class KafkaConfig implements TransportConfig, MergeConfig<KafkaConfig> {
  @Getter @Setter private String topicName;
  @Getter @Setter private String messageKey;

  @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
  @Getter
  @Setter
  private Properties properties;

  KafkaConfig() {
    properties = new Properties();
  }

  @Override
  public KafkaConfig mergeWithNonNull(io.openlineage.client.transports.KafkaConfig other) {
    Properties p = new Properties();
    p.putAll(mergePropertyWith(properties, other.properties));

    return new KafkaConfig(
        mergePropertyWith(topicName, other.topicName),
        mergePropertyWith(messageKey, other.messageKey),
        p);
  }
}
