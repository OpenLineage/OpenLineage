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
   * @deprecated
   *     <p>Since version 1.13.0.
   *     <p>Will be removed in version 1.16.0.
   *     <p>Please use {@link #getMessageKey()} instead
   */
  @Deprecated
  String getLocalServerId() {
    return messageKey;
  }

  /**
   * @deprecated
   *     <p>Since version 1.13.0.
   *     <p>Will be removed in version 1.16.0.
   *     <p>Please use {@link #setMessageKey()} instead
   */
  @Deprecated
  void setLocalServerId(String localServerId) {
    this.messageKey = localServerId;
  }
}
