/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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
  @Getter @Setter private String messageKey;
  @Getter @Setter private Properties properties;

  KafkaConfig() {
    properties = new Properties();
  }

  /**
   * @deprecated Replaced by {@link #getMessageKey()} since v1.13.0, and will be removed in v1.16.0
   */
  @Deprecated
  String getLocalServerId() {
    return messageKey;
  }

  /**
   * @deprecated Replaced by {@link #setMessageKey()} since v1.13.0, and will be removed in v1.16.0
   */
  @Deprecated
  void setLocalServerId(String localServerId) {
    this.messageKey = localServerId;
  }
}
