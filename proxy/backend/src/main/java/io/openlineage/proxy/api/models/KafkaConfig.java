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
