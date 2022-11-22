/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import java.util.Properties;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@ToString
public final class KafkaConfig extends TransportConfig {
  @Getter @Setter private String topicName;
  @Getter @Setter private String localServerId;
  @Getter @Setter private Properties properties;

  public boolean hasLocalServerId() {
    return (localServerId != null);
  }
}
