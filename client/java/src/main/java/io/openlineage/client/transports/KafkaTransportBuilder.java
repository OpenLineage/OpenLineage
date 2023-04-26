/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

public class KafkaTransportBuilder implements TransportBuilder {
  private static final String DEFAULT_LINEAGE_SOURCE = "openlineage-java";

  @Override
  public TransportConfig getConfig() {
    return new KafkaConfig();
  }

  @Override
  public Transport build(TransportConfig config) {
    final KafkaConfig kafkaConfig = (KafkaConfig) config;
    if (!kafkaConfig.hasLocalServerId()) {
      // Set the local server ID to the lineage source when not specified
      kafkaConfig.setLocalServerId(DEFAULT_LINEAGE_SOURCE);
    }
    kafkaConfig.getProperties().put("server.id", kafkaConfig.getLocalServerId());
    return new KafkaTransport(kafkaConfig);
  }

  @Override
  public String getType() {
    return "kafka";
  }
}
