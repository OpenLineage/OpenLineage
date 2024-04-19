/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.proxy.api.models;

import io.openlineage.proxy.ProxyStreamConfig;
import java.util.Properties;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@ToString
public final class KafkaConfig implements ProxyStreamConfig {
  @Getter @Setter private String topicName;
  @Getter @Setter private String messageKey;
  @Getter @Setter private String bootstrapServerUrl;
  @Getter @Setter private Properties properties;

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
