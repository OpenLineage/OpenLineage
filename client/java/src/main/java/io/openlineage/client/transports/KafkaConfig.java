/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import java.util.Properties;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public final class KafkaConfig implements TransportConfig {
  @Getter @Setter private String topicName;
  @Getter @Setter private String localServerId;
  @Getter @Setter private Properties properties;

  KafkaConfig() {
    properties = new Properties();
  }

  public boolean hasLocalServerId() {
    return (localServerId != null);
  }
}
